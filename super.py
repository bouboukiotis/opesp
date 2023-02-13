from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
import json
    
## Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize client
client = ElsClient(config['apikey'])
# client.inst_token = config['insttoken']

## Initialize doc search object using Scopus and execute search, retrieving all results
def docs_per_author():
    
    # doc_srch = ElsSearch("AFFIL(dartmouth) AND AUTHOR-NAME(lewis) AND PUBYEAR > 2011",'scopus')
    # doc_srch = ElsSearch("AF-ID(60012296) AND AU-ID(7005977027) AND PUBYEAR > 2016 AND PUBYEAR < 2023",'scopus')
    # doc_srch = ElsSearch("AF-ID(60012296) AND AU-ID(7005977027) AND PUBYEAR = 2022",'scopus')
    doc_srch = ElsSearch("AF-ID(60012296) AND PUBYEAR = 2022",'scopus')
    doc_srch.execute(client, get_all = True)
    print ("doc_srch has", len(doc_srch.results), "results.")
    # print(doc_srch.results)
    
def author_ID(f_name,l_name):
        # retrieve scopus id from author's name
    # first_name = 'Konstantina'
    # last_name = 'Karathanasopoulou'
    afil_name = 'Harokopio'

    auth_srch = ElsSearch('AUTHLASTNAME(%s)'%l_name + ' AUTHFIRST(%s)'%f_name + ' AFFIL(%s)'%afil_name,'author')
    auth_srch.execute(client)
    
    if (len(auth_srch.results) == 1):
        for author in auth_srch.results:
        #let's look on every author and print the name and affiliaiton stored in Scopus  
            author_id = author['dc:identifier'].split(':')[1]
        
    print(author_id)
    # print ("Found ", len(auth_srch.results), " authors \n")
    # authorfound = auth_srch.results[0]
    # print(authorfound)

    # print('{:<6} {:<6} {:<12} {:<15} {:>}'.format('First name |', 'Last name |', 'Scopus ID |', 'Affil ID', '| Affil name'))
    # print('-'*80)
    # for author in auth_srch.results:
    #     #let's look on every author and print the name and affiliaiton stored in Scopus  
    #     author_id = author['dc:identifier'].split(':')[1]
    #     first_name_scopus = author['preferred-name']['given-name']
    #     last_name_scopus = author['preferred-name']['surname']
    #     affil_name = author['affiliation-current']['affiliation-name']
    #     affil_id = author['affiliation-current']['affiliation-id']
        
    #     print('{:<12} {:<11} {:<14} {:<14} {:>}'.format(first_name_scopus, last_name_scopus, author_id, affil_id, affil_name))

author_ID(f_name='Demosthenes',l_name='Panagiotakos')