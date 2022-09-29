from neo4j import GraphDatabase

uri = "neo4j:////localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "sdm123"))

with driver.session() as session: 
    # Catalog of commands
    node_commands = {
       'university':
            "LOAD CSV WITH HEADERS FROM 'file:///university.csv' AS file " +
            "CREATE (:university {id: file.id, university: file.university})"
            
        , 'company':
            "LOAD CSV WITH HEADERS FROM 'file:///company.csv' AS file " +
            "CREATE (:company {id: file.id, company: file.company})"
     }
        
    index_commands = {
        'company':
            "CREATE INDEX ix_company FOR (n:company) ON n.id" 
            
        , 'university':
            "CREATE INDEX ix_university FOR (n:university) ON n.id"  
    }
        
    edge_commands = {
        'author_affiliatedto_company':
            "LOAD CSV WITH HEADERS FROM 'file:///author_affiliatedto_company.csv' AS file " +
            "MATCH (a:author {id:file.idauthor}), (c:company {id:file.idcompany}) " +
            "CREATE (a)-[:affiliatedto]->(c)"
            
        , 'author_affiliatedto_university':
            "LOAD CSV WITH HEADERS FROM 'file:///author_affiliatedto_university.csv' AS file " +
            "MATCH (a:author {id:file.idauthor}), (u:university {id:file.iduniversity}) " +
            "CREATE (a)-[:affiliatedto]->(u)"
    }

    set_commands = {
        'review_properties':
        "LOAD CSV WITH HEADERS FROM 'file:///author_reviews_paper.csv' AS file " +
        "MATCH (a:author {id:file.authorid})-[r:reviews]->(p:paper {id:file.paperid}) " +
        "SET r.review=file.review, r.timestamp=file.timestamp, r.decision=file.decision"

        , 'journal_properties':
        "LOAD CSV WITH HEADERS FROM 'file:///journal.csv' AS file " +
        "MATCH (j:journal {id:file.id}) " +
        "SET j.numreviewers=file.numreviewers"

        , 'conference_properties':
        "LOAD CSV WITH HEADERS FROM 'file:///conference.csv' AS file " +
        "MATCH (c:conference {id:file.id}) " +
        "SET c.numreviewers=file.numreviewers"
    }
             

    # Execute commands corresponding to the command catalog
    for node in node_commands.keys():
        session.run(node_commands[node])
        
    for index in index_commands.keys():
        session.run(index_commands[index])
        
    for edge in edge_commands.keys():
        session.run(edge_commands[edge])

    for cset in set_commands.keys():
        session.run(set_commands[cset])
    
driver.close()