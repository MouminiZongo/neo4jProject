'''Insert data from collectData.py into a neo4j database and query it.'''

import glob
import json
import neo4j
from neo4j import GraphDatabase, basic_auth
import neo4j.exceptions
import os.path

dataDir = 'data/'
jsonDir = os.path.join(dataDir, 'json')

def main():
    populateNeo4j(jsonDir, True)
    queryNeo4j()


def populateNeo4j(jsonDir, clearDb=False):
    'Load the JSON results from google into neo4j'

    driver = GraphDatabase.driver(
        'bolt://localhost:7687', auth=basic_auth('neo4j', 'cisc4610'))
    session = driver.session()

    # From: https://stackoverflow.com/a/29715865/2037288
    deleteQuery = '''
    MATCH (n)
    OPTIONAL MATCH (n)-[r]-()
    WITH n,r LIMIT 50000
    DELETE n,r
    RETURN count(n) as deletedNodesCount
    '''

    # TODO: complete insert query to include all necessary entities and
    # relationships at once
    insertQuery = '''
    WITH $json as q
    MERGE (img:Image {url:q.url})
        ON CREATE SET img.isDocument = 'true'
        ON MATCH  SET img.isDocument = 'true'
    FOREACH (ann in q.response.labelAnnotations | 
        MERGE (lbl:Label {mid:ann.mid})
            ON CREATE SET lbl.description = ann.description
        MERGE (img)-[:TAGGED {score:ann.score}]->(lbl))
    FOREACH (fmi in q.response.webDetection.fullMatchingImages | 
        MERGE (img2:Image {url:fmi.url})
            ON CREATE SET img2.isDocument = 'false'
        MERGE (img)-[:MATCH {type:'full'}]->(img2))
    FOREACH (pma in q.response.webDetection.partialMatchingImages | 
        MERGE (img2:Image {url:pma.url})
            ON CREATE SET img2.isDocument = 'false'
        MERGE (img)-[:MATCH {type:'partial'}]->(img2))
    FOREACH (web in q.response.webDetection.webEntities | 
        MERGE (ent:WebEntity {entityId:web.entityId})
            ON CREATE SET ent.description = COALESCE(web.description, '')
        MERGE (img)-[:TAGGED {score:web.score}]->(ent))
    FOREACH (pmi in q.response.webDetection.pagesWithMatchingImages | 
        MERGE (pag:Page {url:pmi.url})
        MERGE (img)-[:IN]->(pag))
    FOREACH (lma in q.response.landmarkAnnotations | 
        MERGE (lan:Landmark {mid:lma.mid, 
                             description:COALESCE(lma.description, '')})
        MERGE (img)-[:CONTAINS {score:lma.score}]->(lan)
        FOREACH (loc in lma.locations |
            MERGE (lct:Location {latitude:loc.latLng.latitude, 
                                 longitude:loc.latLng.longitude})
            MERGE (lan)-[:LOCATED_AT]->(lct)))
    '''
    
    countQuery = '''
    MATCH (a) WITH DISTINCT LABELS(a) AS temp, COUNT(a) AS tempCnt
    UNWIND temp AS label
    RETURN label, SUM(tempCnt) AS cnt
    ORDER BY label
    '''

    if clearDb:
        result = session.run(deleteQuery)
        for record in result:
            print('Deleted', record['deletedNodesCount'], 'nodes')

    loaded = 0
    for jsonFile in glob.glob(os.path.join(jsonDir, '*.json')):
        print('Loading', jsonFile, 'into neo4j')
        with open(jsonFile) as jf:
            jsonData = json.load(jf)
            try:
                session.run(insertQuery, {'json': jsonData})
                loaded += 1
            except neo4j.exceptions.ClientError as ce:
                print(' ^^^^ Failed:', str(ce))

    print('\nLoaded', loaded, 'JSON documents into Neo4j\n')

    queryNeo4jAndPrintResults(countQuery, session, 'Neo4j now contains')

    session.close()


def queryNeo4j():
    driver = GraphDatabase.driver(
        'bolt://localhost:7687', auth=basic_auth('neo4j', 'cisc4610'))
    session = driver.session()

    # 0. Count the total number of images in the database
    query_0 = '''
    MATCH (n:Image) RETURN COUNT(n) as cnt
    '''
    queryNeo4jAndPrintResults(query_0, session, title='Query 0')
    
    # TODO: 1. Count the total number of JSON documents in the database
    query_1 = '''
    MATCH (doc:Image {isDocument:'true'}) RETURN COUNT(doc) as cnt
    '''
    queryNeo4jAndPrintResults(query_1, session, title='Query 1')

    # TODO: 2. Count the total number of Images, Labels, Landmarks, 
    # Locations, Pages, and Web Entities in the database, listing the count 
    # for each node label separately.
    query_2 = ''' // this solution for this query was given in the starter code
    MATCH (a) WITH DISTINCT LABELS(a) AS temp, COUNT(a) AS tempCnt
    UNWIND temp AS label
    RETURN label, SUM(tempCnt) AS cnt
    ORDER BY label
    '''
    queryNeo4jAndPrintResults(query_2, session, title='Query 2')

    # TODO: 3. List all Images tagged with the Label with mid '/m/015kr' (which 
    # has the description 'bridge'), ordered by the score of the association 
    # between them from highest to lowest
    query_3 = '''
    MATCH (lbl:Label {mid: '/m/015kr'}) <-[tag:TAGGED]- (img:Image) 
    RETURN img.url
    ORDER BY tag.score DESC
    '''
    queryNeo4jAndPrintResults(query_3, session, title='Query 3')

    # TODO: 4. List the 10 most frequent WebEntities that are applied
    # to the same Images as the Label with an id of '/m/015kr' (which
    # has the description 'bridge'). List them in descending order of
    # the number of times they appear, followed by their entityId
    # alphabetically
    query_4 = '''
    MATCH (lbl:Label {mid: '/m/015kr'}) <-[:TAGGED]- (img:Image) 
    MATCH (img) -[tag:TAGGED]->(web:WebEntity)
    RETURN web.entityId as eid, COUNT(tag) as cnt
    ORDER BY cnt DESC, eid
    LIMIT 10
    '''
    queryNeo4jAndPrintResults(query_4, session, title='Query 4')

    # TODO: 5. Find all Images associated with any Landmarks that are not 'New
    # York' or 'New York City', ordered alphabetically by landmark description 
    # and then by image URL.
    query_5 = '''
    MATCH (lma:Landmark) <-[:CONTAINS]- (img:Image)
    WHERE NOT lma.description IN ['New York', 'New York City']
    RETURN lma.description, img.url
    ORDER BY lma.description, img.url
    '''
    queryNeo4jAndPrintResults(query_5, session, title='Query 5')

    # TODO: 6. List the 10 Labels that have been applied to the most
    # Images along with the number of Images each has been applied to
    query_6 = '''
    MATCH (lbl:Label) <-[:TAGGED]- (img:Image)
    RETURN lbl.description, COUNT(img) as cnt
    ORDER BY cnt DESC
    LIMIT 10
    '''
    queryNeo4jAndPrintResults(query_6, session, title='Query 6')

    # TODO: 7. List the 10 Pages that are linked to the most Images
    # through the webEntities.pagesWithMatchingImages JSON property
    # along with the number of Images linked to each one. Sort them by
    # count (descending) and then by page URL.
    query_7 = '''
    MATCH (img:Image) -[:IN]-> (pag:Page)
    RETURN pag.url, COUNT(img) as cnt
    ORDER BY cnt DESC, pag.url
    LIMIT 10
    '''
    queryNeo4jAndPrintResults(query_7, session, title='Query 7')

    # TODO: 8. List the 10 pairs of Images that appear on the most Pages 
    # together through the webEntities.pagesWithMatchingImages JSON property. 
    # List them in descending order of the number of pages that they appear on 
    # together, then by the URL of the first image. Make sure that each pair is 
    # only listed once regardless of which is first and which is second.
    query_8 = '''
    MATCH (img1:Image) -[:IN]-> (pag:Page)
    MATCH (img2:Image) -[:IN]-> (pag)
    WHERE img1.url < img2.url
    RETURN img1.url, img2.url, COUNT(pag) as cnt
    ORDER BY cnt DESC, img1.url
    LIMIT 10
    '''
    queryNeo4jAndPrintResults(query_8, session, title='Query 8')

    # All done!
    session.close()


def queryNeo4jAndPrintResults(query, session, title='Running query:'):
    print()
    print(title)
    print(query)

    if not query.strip():
        return
    
    for record in session.run(query):
        print(' ' * 4, end='')
        for val in record:
            print(val, end='\t')
        print()


if __name__ == '__main__':
    main()
