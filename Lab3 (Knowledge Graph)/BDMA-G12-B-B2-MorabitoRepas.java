import org.apache.jena.ontology.Individual;
import org.apache.jena.ontology.OntClass;
import org.apache.jena.ontology.OntModel;
import org.apache.jena.ontology.OntProperty;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.jena.ontology.OntModelSpec.OWL_MEM;

public class AboxGenerator {

    private static final String TBOX = "tbox.ttl";
    private static final String ABOX = "abox.nt";
    private static final String URL = "http://www.sdm.com/schema/";
    private static final String ID_SEPARATOR = "@";

    // FOR NODES
    private static final Map<String, List<String>> CLASS_TO_PROPERTIES = Map.of(
            "Author", List.of("personfullname"),
            "Paper", List.of("papertitle"),
            "Area", List.of("areaname"),
            "Review", List.of("reviewdecision", "reviewcomment"),
            "Conference", List.of("venuename", "numseats"),
            "Journal", List.of("venuename", "numawards"),
            "Venuehead", List.of("personfullname")
    );
    private static final Map<String, List<Integer>> CLASS_TO_ID_INDICES = Map.of(
            "Author", List.of(0),
            "Paper", List.of(0),
            "Area", List.of(0),
            "Review", List.of(0, 1),
            "Conference", List.of(0),
            "Journal", List.of(0),
            "Venuehead", List.of(0)
    );
    private static final Map<String, Function<String[], ?>> PROPERTY_TO_GET_VALUE_FUNC = Map.of(
            "personfullname", row -> row[1],
            "papertitle", row -> row[1],
            "areaname", row -> row[1],
            "reviewdecision", row -> Boolean.parseBoolean(row[4]),
            "reviewcomment", row -> row[2],
            "venuename", row -> row[1],
            "numseats", row -> Integer.parseInt(row[2]),
            "numawards", row -> Integer.parseInt(row[2])
    );

    // FOR EDGES
    private static final List<String> EDGES_LIST = List.of("paper_about_area", "author_writes_paper",
            "paper_publishedin_journal", "paper_publishedin_conference", "conference_focuseson_area",
            "venuehead_handles_conference", "venuehead_handles_journal", "venuehead_assigns_review"
    );


    private static void createNodes(OntModel base) {
        for (String className : CLASS_TO_PROPERTIES.keySet()) {
            OntClass class_ = base.getOntClass(URL + className);
            List<OntProperty> properties = getPropertiesOfClass(base, className);

            for (String[] row : readCsv(className.toLowerCase() + ".csv")) {
                String id = getIdFromRow(row, CLASS_TO_ID_INDICES.get(className));

                Individual instance = base.createIndividual(URL + className.toLowerCase() + id, class_);
                for (OntProperty property : properties) {
                    Function getValueFunc = PROPERTY_TO_GET_VALUE_FUNC.get(property.getLocalName());
                    instance.addProperty(property, base.createTypedLiteral(getValueFunc.apply(row)));
                }

                // for reviews (since the "involvedIn" and "isUnder" relationships are in the same file)
                if (id.contains(ID_SEPARATOR)) {
                    addPropertiesInReviewFile(base, row, instance);
                }
            }
        }
    }

    private static void addPropertiesInReviewFile(OntModel base, String[] row, Individual instance) {
        Resource reviewer = base.getResource(URL + "author" + row[0]);
        Resource paper = base.getResource(URL + "paper" + row[1]);
        // reviewer -> review
        OntProperty involvedIn = base.getOntProperty(URL + "involvedin");
        reviewer.addProperty(involvedIn, instance);
        // paper -> review
        OntProperty isUnder = base.getOntProperty(URL + "isunder");
        paper.addProperty(isUnder, instance);
    }

    private static List<OntProperty> getPropertiesOfClass(OntModel base, String className) {
        return CLASS_TO_PROPERTIES.get(className)
                .stream()
                .map(propertyName -> base.getOntProperty(URL + propertyName))
                .collect(Collectors.toList());
    }

    private static String getIdFromRow(String[] row, List<Integer> indices) {
        return indices.stream()
                .map(index -> row[index])
                .collect(Collectors.joining(ID_SEPARATOR));
    }

    private static void createEdges(OntModel base) {
        for (String triple : EDGES_LIST) {
            String[] splitFilename = triple.split("_");
            String className1 = splitFilename[0];
            String edgeName = splitFilename[1];
            String className2 = splitFilename[2];
            OntProperty edge = base.getOntProperty(URL + edgeName);

            for (String[] row : readCsv(triple + ".csv")) {
                Resource instance1 = base.getResource(URL + className1 + row[0]);
                Resource instance2 = base.getResource(URL + className2 + row[1]);
                // add edge from instance1 to instance2
                instance1.addProperty(edge, instance2);
            }
        }
    }

    private static List<String[]> readCsv(String csvName) {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader("resources/" + csvName))) {
            String row;
            br.readLine(); //skip header
            while ((row = br.readLine()) != null) {
                String[] values = getValuesFromRow(row);
                if (values.length > 1 && row.charAt(0) != ' ' && row.charAt(0) != '\t')
                    rows.add(values);
            }
        } catch (Exception e) {
            System.out.println("Error with file: " + csvName);
            e.printStackTrace();
        }
        return rows;
    }

    private static String[] getValuesFromRow(String row) {
        // returns the values in a csv row splitting on the comma ONLY IF it has zero or a even number of quotes ahead of it
        return row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    }

    private static void saveStatements(Model model) {
        try {
            RDFDataMgr.write(new FileOutputStream(ABOX), model, Lang.NTRIPLES);
        } catch (FileNotFoundException e) {
            System.out.println("ERROR IN SAVING THE STATEMENTS.\tThe statements are printed below:\n");
            printStatements(model);
        }
    }

    private static void printStatements(Model model) {
        RDFDataMgr.write(System.out, model, Lang.NTRIPLES);
    }

    public static void main(String[] args) {
        OntModel base = ModelFactory.createOntologyModel(OWL_MEM);
        base.read(TBOX);

        createNodes(base);
        createEdges(base);

        saveStatements(base);
    }

}