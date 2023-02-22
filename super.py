import inspect
import json
import pandas as pd
pd.set_option('display.max_columns', None)

from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch

# from pybliometrics.scopus import AbstractRetrieval
# from pybliometrics.scopus import AffiliationSearch
# from pybliometrics.scopus import CitationOverview

from pybliometrics.scopus import *
from pyscopus import Scopus
from datetime import date

apikey = '10f6bba55e83f58854aba8ce886cd636'
scopus = Scopus(apikey)
client = ElsClient(apikey)
affiliation_hua_name = 'Harokopio'
affiliation_hua_id = "60012296"
current_year = date.today().year

# retrieve scopus id from author's name
def find_scopus_ID(first_name,last_name,df,i):

    # print(str(first_name),str(last_name))
    auth_srch = ElsSearch('AUTHLASTNAME(%s)'%last_name + ' AUTHFIRST(%s)'%first_name + ' AFFIL(%s)'%affiliation_hua_name,'author')
    auth_srch.execute(client)

    for author in auth_srch.results:
        try:
            #search for each author and by each name and affiliaiton stored in Scopus  
            author_id = author['dc:identifier'].split(':')[1]
            first_name_scopus = author['preferred-name']['given-name']
            last_name_scopus = author['preferred-name']['surname']
            
            # save first name, last name and scopus ID as they are in scopus !!!
            df.loc[i, 'fname'] = first_name_scopus
            df.loc[i, 'lname'] = last_name_scopus
            df.loc[i, 'scopus_ID'] = author_id

        except:
            df.loc[i, 'scopus_ID'] = "NA"
            continue

def batch_search_authors(df_members_list):
   
    i=0 # for iteration purpose of dataframe in order to edit scopus ID values

    # iterate dataframe and extract first name and last name
    for row in df_members_list.itertuples():
        
        fname = row[1]
        lname = row[2]

        find_scopus_ID(fname.strip(), lname.strip(),df_members_list,i)
        i+=1

    return(df_members_list)

def batch_search_citations(author_id, year_range, citation_type):
   
    
    i=0 # for iteration purpose of dataframe in order to edit scopus ID values

    if (year_range == 1):

        year = current_year - 1
        search_df = scopus.search('AU-ID(%s)'%author_id + ' AND AF-ID(%s)'%affiliation_hua_id, count=9000)
        # search_df.to_csv("/home/pboump/PROJECTS/Python/scopus2/elsapy/files/pubs_auth.csv",sep=';')
   
        # iterate dataframe and extract first name and last name
        for row in search_df.itertuples():
            
            doc_year = row[9].split("-")
            doc_scopus_id = row[1]

            if (int(year) == int(doc_year[0])):
                print(doc_year[0],doc_scopus_id)
                co = CitationOverview(doc_scopus_id,id_type='scopus_id',start=1970,end=2023,refresh=True, citation="exclude-self")
                # print(co.rangeCount)
            # find_scopus_ID(fname.strip(), lname.strip(),df_author_docs,i)
            i+=1

    #     # search_df = scopus.search("AU-ID(56500820900) AND AF-ID(60012296)", count=9000)
    #     # search_df.to_csv("/home/pboump/projects/scopus/elsapy/files/pubs_auth.csv",sep=';')

    #     # identifier = ["10.1016/S0140-6736(10)60484-9"]
    #     identifier = ["85119091250"]
    #     co = CitationOverview(identifier,id_type='scopus_id',start=2017,end=2022,refresh=True, citation="exclude-self")
    #     print(co.rangeCount)

    # return(df_author_docs)

## Initialize doc search object using Scopus and execute search, retrieving all results
def author_docs(author_id, year_range):
    
    if (year_range == 1):

        year = current_year - 1
        doc_srch = ElsSearch('AF-ID(%s)'%affiliation_hua_id + ' AU-ID(%s)'%author_id + ' AND PUBYEAR = %s'%year,'scopus')
    
    else:
        year_a = (current_year - (year_range + 2))
        doc_srch = ElsSearch('AF-ID(%s)'%affiliation_hua_id + ' AU-ID(%s)'%author_id + ' AND PUBYEAR > %s'%year_a + ' AND PUBYEAR < %s'%current_year,'scopus')
        # doc_srch = ElsSearch('AU-ID(%s)'%author_id + ' AND PUBYEAR > %s'%year_a + ' AND PUBYEAR < %s'%current_year,'scopus')
        
    doc_srch.execute(client, get_all = True)

    # Fix result numbers if no answer found 
    if (str(doc_srch.results[0]) == "{'@_fa': 'true', 'error': 'Result set was empty'}"):
    # if ("empty" in str(doc_srch.results[0])):
        result = len(doc_srch.results) -1
    else:
        result = len(doc_srch.results)

    print ("Author has", result, "documents in this year range.")



# author_scopus_id = '56500820900'
# status, search_result, npubs, ncits, hindex = auth_metrics(author_scopus_id, client)

# print(search_result['author-profile']['preferred-name']['given-name'], search_result['author-profile']['preferred-name']['surname'])
# print("Number of publications %i."%npubs, "Citations %i."%ncits, " h-index %i."%hindex)


# set HUA members file (per staff status and per department)
file_type = "uni_no_dept"

# demonstrate the filepath according to each name
# filename = pd.read_csv("/home/pboump/projects/scopus/elsapy/files/"+str(file_type)+".csv",sep=';')

# results_df = batch_search_authors(filename)

# # export results to csv
# results_df.to_csv("/home/pboump/projects/scopus/elsapy/files/"+str(file_type)+"_ID.csv",sep=';')

# print(results_df)

# author_docs('56500820900',15)

# #################################################################################################################
# search_df = scopus.search("AU-ID(56500820900) AND AF-ID(60012296)", count=9000)
# search_df.to_csv("/home/pboump/PROJECTS/Python/scopus2/elsapy/files/pubs_auth.csv",sep=';')

# # identifier = ["10.1016/S0140-6736(10)60484-9"]
# identifier = ["85119091250"]
# co = CitationOverview(identifier,id_type='scopus_id',start=1970,end=2022,refresh=True, citation="exclude-self")
# print(co.rangeCount)
# #################################################################################################################

batch_search_citations(author_id='56500820900',year_range=1,citation_type=None)





# ab = AbstractRetrieval("10.1016/j.softx.2019.100263")
# print(ab.title)

# author_scopus_id = '56500820900'
# status, search_result, npubs, ncits, hindex = auth_metrics(author_scopus_id, client)




# ## Load configuration
# con_file = open("config.json")
# config = json.load(con_file)
# con_file.close()


# ## Initialize doc search object using Scopus and execute search, retrieving 
# #   all results
# doc_srch = ElsSearch("AFFIL(dartmouth) AND AUTHOR-NAME(lewis) AND PUBYEAR > 2011",'scopus')
# doc_srch.execute(client, get_all = True)
# print ("doc_srch has", len(doc_srch.results), "results.")

