from neo4j import GraphDatabase

uri = "neo4j:////localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "sdm123"))

with driver.session() as session: 
    # Catalog of commands
    node_commands = {
        'conference': 
         "LOAD CSV WITH HEADERS FROM 'file:///conference.csv' AS file " +
         "CREATE (:conference {id: file.id, name: file.name, numreviewers: file.numreviewers})"
         
         , 'edition':
             "LOAD CSV WITH HEADERS FROM 'file:///edition.csv' AS file " +
             "CREATE (:edition {id: file.id, name: file.name, number: file.number, city: file.city})"
             
         , 'journal':
             "LOAD CSV WITH HEADERS FROM 'file:///journal.csv' AS file " +
             "CREATE (:journal {id: file.id, name: file.name, numreviewers: file.numreviewes})"
             
         , 'volume':
             "LOAD CSV WITH HEADERS FROM 'file:///volume.csv' AS file " +
             "CREATE (:volume {id: file.id, name: file.title})"
             
        , 'paper':
            "LOAD CSV WITH HEADERS FROM 'file:///paper.csv' AS file " +
            "CREATE (:paper {id: file.id, title: file.title, year: toInteger(file.year), language: file.lang, isbn: file.isbn, abstract: file.abstract})"
            
        , 'author':
            "LOAD CSV WITH HEADERS FROM 'file:///author.csv' AS file " +
            "CREATE (:author {id: file.id, name: file.name, email: file.email})"
            
        , 'topic':
            "LOAD CSV WITH HEADERS FROM 'file:///topic.csv' AS file " +
            "CREATE (:topic {id: file.id, name: file.topic})"   
     }
        
    index_commands = {
        'edition':
            "CREATE INDEX ix_edition FOR (n:edition) ON n.id"
            
        , 'paper':
            "CREATE INDEX ix_paper FOR (n:paper) ON n.id"
            
        , 'conference':
            "CREATE INDEX ix_conference FOR (n:conference) ON n.id"
            
        , 'journal':
            "CREATE INDEX ix_journal FOR (n:journal) ON n.id" 
            
        , 'volume':
            "CREATE INDEX ix_volume FOR (n:volume) ON n.id"
            
        , 'author':
            "CREATE INDEX ix_author FOR (n:author) ON n.id"
            
        , 'topic':
            "CREATE INDEX ix_topic FOR (n:topic) ON n.id"
    }
        
    edge_commands = {
        'conference_compose_edition':
            "LOAD CSV WITH HEADERS FROM 'file:///conference_composed_edition.csv' AS file " +
            "MATCH (c:conference {id:file.idconference}), (e:edition {id:file.idedition}) " +
            "CREATE (c)-[:composed]->(e)"
            
        , 'volume_ispartof_journal':
            "LOAD CSV WITH HEADERS FROM 'file:///volume_ispartof_journal.csv' AS file " +
            "MATCH (v:volume {id:file.idvolume}), (j:journal {id:file.idjournal}) " +
            "CREATE (v)-[:ispartof]->(j)"
            
        , 'edition_includes_paper':
            "LOAD CSV WITH HEADERS FROM 'file:///edition_includes_paper.csv' AS file " +
            "MATCH (e:edition {id:file.idedition}), (p:paper {id:file.idpaper}) " +
            "CREATE (e)-[:includes]->(p)"
            
        , 'paper_appears_volume':
            "LOAD CSV WITH HEADERS FROM 'file:///paper_appears_volume.csv' AS file " +
            "MATCH (v:volume {id:file.idvolume}), (p:paper {id:file.idpaper}) " +
            "CREATE (p)-[:appears]->(v)"

        , 'paper_about_topic':
            "LOAD CSV WITH HEADERS FROM 'file:///paper_about_topic.csv' AS file " +
            "MATCH (p:paper {id:file.paperid}), (t:topic {id:file.topicid})" +
            "CREATE (p)-[:about]->(t)"
            
        , 'author_writes_paper':
            "LOAD CSV WITH HEADERS FROM 'file:///author_writes_paper.csv' AS file " +
            "MATCH (a:author {id:file.authorid}), (p:paper {id:file.paperid})" +
            "CREATE (a)-[:writes]->(p)"
            
        , 'author_reviews_paper':
            "LOAD CSV WITH HEADERS FROM 'file:///author_reviews_paper.csv' AS file " +
            "MATCH (a:author {id:file.authorid}), (p:paper {id:file.paperid}) " +
            "CREATE (a)-[:reviews]->(p)"
            
        , 'paper_cites_paper':
            "LOAD CSV WITH HEADERS FROM 'file:///paper_cites_paper.csv' AS file " +
            "MATCH (p1:paper {id:file.paperidsrc}), (p2:paper {id:file.paperidref}) " +
            "CREATE (p1)-[:cites]->(p2)"
            
        , 'author_owns_paper':
            "LOAD CSV WITH HEADERS FROM 'file:///author_of_paper.csv' AS file " +
            "MATCH (a:author {id:file.authorid}), (p:paper {id:file.paperid}) " +
            "CREATE (a)-[:owns]->(p)"
    }
             

    # Execute commands corresponding to the command catalog
    for node in node_commands.keys():
        session.run(node_commands[node])
        
    for index in index_commands.keys():
        session.run(index_commands[index])
        
    for edge in edge_commands.keys():
        session.run(edge_commands[edge])
    
driver.close()