import inspect
import json
import pandas as pd
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

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
reference_year = current_year - 1
# year_start = (current_year - (year_range + 1))

# split list with author docs ids into chunks
def divide_chunks(l, n):
     
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

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


def count_author_docs(author_id, year_range):
    i=0 # for iteration purpose of dataframe in order to edit scopus ID values
    author_docs_id=[]
    count = 0
    # doc_year = 1970;

    # reference_year = current_year - 1
    year_start = (current_year - (year_range + 1))
    search_df = scopus.search('AU-ID(%s)'%author_id + ' AND AF-ID(%s)'%affiliation_hua_id, count=9000)
    search_df.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/pubs_auth_" + str(author_id) + ".csv",sep=';')
    exception = "book"
    # iterate dataframe and extract
    for row in search_df.itertuples():
        
        doc_year = row[9].split("-")
        doc_scopus_id = row[1]
        doc_type = row[13]        

        # exclude books from author documents according to M1.265/M1.266 indicators from OPESP
        if doc_type != None and exception not in doc_type.casefold():
            if year_range == 1:
                if int(reference_year) == int(doc_year[0]):
                    author_docs_id.append(doc_scopus_id)
                    count+=1
            else:
                if (int(doc_year[0]) > int(year_start) and int(doc_year[0]) <= int(reference_year)):
                    author_docs_id.append(doc_scopus_id)
                    count+=1
        i+=1
    print(len(author_docs_id))


def count_multiple_authors_docs(dept_members_file, year_range):
    i=0 # for iteration purpose of dataframe in order to edit scopus ID values
    author_docs_id=[]
    count = 0
    # doc_year = 1970;

    # reference_year = current_year - 1
    # reference_year = 2021
    year_start = (current_year - (year_range + 1))

    df_dept_members = pd.read_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/"+str(dept_members_file),sep=';',dtype=str)

    for row in df_dept_members.itertuples():
        author_scopus_ID = str(row[5])

        if ("nan" not in author_scopus_ID):
            print (author_scopus_ID)
            search_df = scopus.search('AU-ID(%s)'%author_scopus_ID + ' AND AF-ID(%s)'%affiliation_hua_id, count=9000)
            # search_df.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/DIT_pubs_auth_" + str(author_scopus_ID) + ".csv",sep=';')
            search_df.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/DEPT_TOTAL_PUBS_" +str(dept_members_file),sep=';',mode='a')
            exception = "book"
            # iterate dataframe and extract
            for row in search_df.itertuples():
                
                doc_year = row[9].split("-")
                doc_scopus_id = row[1]
                doc_type = row[13]        

                # exclude books from author documents according to M1.265/M1.266 indicators from OPESP
                if doc_type != None and exception not in doc_type.casefold():
                    if year_range == 1:
                        if int(reference_year) == int(doc_year[0]):
                            author_docs_id.append(doc_scopus_id)
                            count+=1
                    else:
                        if (int(doc_year[0]) > int(year_start) and int(doc_year[0]) <= int(reference_year)):
                            author_docs_id.append(doc_scopus_id)
                            count+=1
        i+=1
        
    return (len(set(author_docs_id))), author_docs_id

def batch_search_citations(author_id, year_range, citation_type):
   
    i=0 # for iteration purpose of dataframe in order to edit scopus ID values
    author_docs_id=[]

    doc_year = 1970;

    # reference_year = current_year - 1
    year_start = (current_year - (year_range + 1))
    search_df = scopus.search('AU-ID(%s)'%author_id + ' AND AF-ID(%s)'%affiliation_hua_id, count=9000)
    search_df.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/pubs_auth_" + str(author_id) + ".csv",sep=';')
    exception = "book"
    # iterate dataframe and extract
    for row in search_df.itertuples():
        
        doc_year = row[9].split("-")
        doc_scopus_id = row[1]
        doc_type = row[13]       
        # print(doc_type) 

        # exclude books from author documents according to M1.265/M1.266 indicators from OPESP
        if doc_type != None and exception not in doc_type.casefold():
            author_docs_id.append(doc_scopus_id)

        # An xreiastei na lamvanontai ypopsin anafores gia dimosieyseis poy eginan to teleytaio etos i ta x eti mono
        # Den to thelo giati me noiazei poses anafores eginan to teleytaio xrono gia to synolo ton dimosieyseon, akoma kai an eginan to 1991
        # if (int(reference_year) == int(doc_year[0])):
        #     # print(doc_year[0],doc_scopus_id)

        #     author_docs_id.append(doc_scopus_id)
        #     # co = CitationOverview(doc_scopus_id,id_type='scopus_id',start=1970,end=2023,refresh=True, citation="exclude-self")
        #     # print(co.rangeCount)
        # # find_scopus_ID(fname.strip(), lname.strip(),df_author_docs,i)
        i+=1

    # divide search list into chunks of 25 items in order to search scopus through its API (max=25)
    auth_docs_id_chunks = list(divide_chunks(author_docs_id, 25))
    # print (auth_docs_id_chunks[0],len(auth_docs_id_chunks))
    
    citations_sum = 0
    citations_without_self_sum = 0

    for chunk in range(len(auth_docs_id_chunks)):

        # year_range == 1 : mono to imerologiako etos anaforas pou einai to proigoumeno ap ayto poy ginetai i ereyna (e.g. to 2023 thelo apotelesmata mono gia to 2022)
        if (year_range == 1):
            co = CitationOverview(auth_docs_id_chunks[chunk],id_type='scopus_id',start=reference_year,end=reference_year,refresh=True, citation=None)
            co2 = CitationOverview(auth_docs_id_chunks[chunk],id_type='scopus_id',start=reference_year,end=reference_year,refresh=True, citation="exclude-self")
        
        # year_range != 1 : mono to imerologiako etos anaforas pou einai to proigoumeno ap ayto poy ginetai i ereyna (e.g. to 2023 thelo apotelesmata mono gia to 2022)
        else:
            co = CitationOverview(auth_docs_id_chunks[chunk],id_type='scopus_id',start=year_start,end=reference_year,refresh=True, citation=None)
            co2 = CitationOverview(auth_docs_id_chunks[chunk],id_type='scopus_id',start=year_start,end=reference_year,refresh=True, citation="exclude-self")
        
        citations_sum+=sum(co.rangeCount)
        citations_without_self_sum+=sum(co2.rangeCount)
        
    print("anafores: ", citations_sum)
    clean_citations = citations_sum - citations_without_self_sum
    print("eteroanafores: ", clean_citations)


    #     # search_df = scopus.search("AU-ID(56500820900) AND AF-ID(60012296)", count=9000)
    #     # search_df.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/filespubs_auth.csv",sep=';')

    #     # identifier = ["10.1016/S0140-6736(10)60484-9"]
    #     identifier = ["85119091250"]
    #     co = CitationOverview(identifier,id_type='scopus_id',start=2017,end=2022,refresh=True, citation="exclude-self")
    #     print(co.rangeCount)

    # return(df_author_docs)

## Initialize doc search object using Scopus and execute search, retrieving all results
def author_docs(author_id, year_range):
    
    if (year_range == 1):

        # reference_year = current_year - 1
        doc_srch = ElsSearch('AF-ID(%s)'%affiliation_hua_id + ' AU-ID(%s)'%author_id + ' AND PUBYEAR = %s'%reference_year,'scopus')
    
    else:
        year_start = (current_year - (year_range + 1))
        doc_srch = ElsSearch('AF-ID(%s)'%affiliation_hua_id + ' AU-ID(%s)'%author_id + ' AND PUBYEAR > %s'%year_start + ' AND PUBYEAR < %s'%current_year,'scopus')
        # doc_srch = ElsSearch('AU-ID(%s)'%author_id + ' AND PUBYEAR > %s'%year_start + ' AND PUBYEAR < %s'%current_year,'scopus')
        
    doc_srch.execute(client, get_all = True)

    # Fix result numbers if no answer found 
    if (str(doc_srch.results[0]) == "{'@_fa': 'true', 'error': 'Result set was empty'}"):
    # if ("empty" in str(doc_srch.results[0])):
        result = len(doc_srch.results) -1
    else:
        result = len(doc_srch.results)

    print ("Author has", result, "documents in this reference_year range.")

def count_multiple_citations(dept_members_file,year_range):
   
    i=0 # for iteration purpose of dataframe in order to edit scopus ID values
    author_docs_id =[]

    # df_hua_members_docs_5_dedub = pd.read_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/df_hua_members_docs_5_dedub.csv",sep=';')
    # df_hua_members_docs_1_dedub = pd.read_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/df_hua_members_docs_1_dedub.csv",sep=';')
    
    # search from scopus
    # search_df = scopus.search('AF-ID(%s)'%affiliation_hua_id, count=9000)
    # search_df.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/HUA_pubs_" + ".csv",sep=';')
    # search from stored file
    search_df = pd.read_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/DEPT_TOTAL_PUBS_" +str(dept_members_file),sep=';')
    exception = "book"
    
    # delete rows with headers (multiple header due to files concatenation)
    # indexAge = search_df[ (exception not in search_df['subtype_description'].casefold()) | (search_df['Position'] == 'SG') ].index
    indexAge = search_df[ (search_df['scopus_id'] == 'scopus_id') ].index
    search_df.drop(indexAge , inplace=True)

    # iterate dataframe and extract
    for row in search_df.itertuples():
        
        doc_scopus_id = row[2]
        doc_type = row[15]       
        # print(doc_type) 
        
        # exclude books from author documents according to M1.265/M1.266 indicators from OPESP
        if doc_type != None and exception not in doc_type.casefold():
            author_docs_id.append(doc_scopus_id)

        i+=1

    # print(len(author_docs_id))
    
    # divide search list into chunks of 25 items in order to search scopus through its API (max=25)
    docs_id_chunks = list(divide_chunks(author_docs_id, 25))
    # # print (auth_docs_id_chunks[0],len(auth_docs_id_chunks))
    
    citations_sum = 0
    citations_without_self_sum = 0
    year_start = (current_year - (year_range + 1))

    for chunk in range(len(docs_id_chunks)):

    #     # year_range == 1 : mono to imerologiako etos anaforas pou einai to proigoumeno ap ayto poy ginetai i ereyna (e.g. to 2023 thelo apotelesmata mono gia to 2022)
        if (year_range == 1):
            co = CitationOverview(docs_id_chunks[chunk],id_type='scopus_id',start=reference_year,end=reference_year,refresh=True, citation=None)
            co2 = CitationOverview(docs_id_chunks[chunk],id_type='scopus_id',start=reference_year,end=reference_year,refresh=True, citation="exclude-self")
        
    #     # year_range != 1 : mono to imerologiako etos anaforas pou einai to proigoumeno ap ayto poy ginetai i ereyna (e.g. to 2023 thelo apotelesmata mono gia to 2022)
        else:
            co = CitationOverview(docs_id_chunks[chunk],id_type='scopus_id',start=year_start,end=reference_year,refresh=True, citation=None)
            co2 = CitationOverview(docs_id_chunks[chunk],id_type='scopus_id',start=year_start,end=reference_year,refresh=True, citation="exclude-self")
        
        citations_sum+=sum(co.rangeCount)
        citations_without_self_sum+=sum(co2.rangeCount)
        
    # print("anafores: ", citations_sum)
    # clean_citations = citations_sum - citations_without_self_sum
    # print("eteroanafores: ", clean_citations)
    
    return citations_sum,citations_without_self_sum


  
    # return(df_author_docs)

## Initialize doc search object using Scopus and execute search, retrieving all results

# author_scopus_id = '56500820900'
# status, search_result, npubs, ncits, hindex = auth_metrics(author_scopus_id, client)

# print(search_result['author-profile']['preferred-name']['given-name'], search_result['author-profile']['preferred-name']['surname'])
# print("Number of publications %i."%npubs, "Citations %i."%ncits, " h-index %i."%hindex)


# set HUA members file (per staff status and per department)
file_type = "uni_no_dept"

# demonstrate the filepath according to each name
# filename = pd.read_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files"+str(file_type)+".csv",sep=';')

# results_df = batch_search_authors(filename)

# # export results to csv
# results_df.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files"+str(file_type)+"_ID.csv",sep=';')

# print(results_df)

# author_docs('56500820900',15)

# #################################################################################################################
# search_df = scopus.search("AU-ID(56500820900) AND AF-ID(60012296)", count=9000)
# search_df.to_csv("/home/pboump/PROJECTS/Python/scopus2/elsapy/files/pubs_auth.csv",sep=';')

# # identifier = ["10.1016/S0140-6736(10)60484-9"]
# identifier = ["85143310314"]
# co = CitationOverview(identifier,id_type='scopus_id',start=1970,end=2022,refresh=True, citation="exclude-self")
# print(co.rangeCount)
# #################################################################################################################

# batch_search_citations(author_id='56500820900',year_range=5,citation_type=None)
# batch_search_citations(author_id='6603137301',year_range=5,citation_type=None)
# batch_search_citations(author_id='57194042339',year_range=5,citation_type=None)
# count_author_docs(author_id='56500820900',year_range=5)

# #################################################################################################################

# dit_members_docs_num_1, dit_members_docs_list_1 = count_multiple_authors_docs("dept_dit_ID.csv",1)
# geo_members_docs_num_1, geo_members_docs_list_1 = count_multiple_authors_docs("dept_geo_ID.csv",1)
# tova_members_docs_num_1, tova_members_docs_list_1 = count_multiple_authors_docs("dept_tova_ID.csv",1)
# tedd_members_docs_num_1, tedd_members_docs_list_1 = count_multiple_authors_docs("dept_tedd_ID.csv",1)

# dit_members_docs_num_5, dit_members_docs_list_5 = count_multiple_authors_docs("dept_dit_ID.csv",5)
# geo_members_docs_num_5, geo_members_docs_list_5 = count_multiple_authors_docs("dept_geo_ID.csv",5)
# tova_members_docs_num_5, tova_members_docs_list_5 = count_multiple_authors_docs("dept_tova_ID.csv",5)
# tedd_members_docs_num_5, tedd_members_docs_list_5 = count_multiple_authors_docs("dept_tedd_ID.csv",5)

# print("Δείκτης Μ1.265 (Εργασίες με κριτές scopus σωρευτικά τα τελευταία 5 έτη) - ΤΟΒΑ: ",tova_members_docs_num_5)
# print("Δείκτης Μ1.266 (Εργασίες με κριτές scopus το έτος αναφοράς) - ΤΟΒΑ: ",tova_members_docs_num_1)


# print("Δείκτης Μ1.265 (Εργασίες με κριτές scopus σωρευτικά τα τελευταία 5 έτη) - ΤΓΕΩ: ",geo_members_docs_num_5)
# print("Δείκτης Μ1.266 (Εργασίες με κριτές scopus το έτος αναφοράς) - ΤΓΕΩ: ",geo_members_docs_num_1)


# print("Δείκτης Μ1.265 (Εργασίες με κριτές scopus σωρευτικά τα τελευταία 5 έτη) - ΤΕΔΔ: ",tedd_members_docs_num_5)
# print("Δείκτης Μ1.266 (Εργασίες με κριτές scopus το έτος αναφοράς) - ΤΕΔΔ: ",tedd_members_docs_num_1)


# print("Δείκτης Μ1.265 (Εργασίες με κριτές scopus σωρευτικά τα τελευταία 5 έτη) - ΤΠΤ: ",dit_members_docs_num_5)
# print("Δείκτης Μ1.266 (Εργασίες με κριτές scopus το έτος αναφοράς) - ΤΠΤ: ",dit_members_docs_num_1)


# hua_members_docs_5 = dit_members_docs_list_5 + geo_members_docs_list_5 + tova_members_docs_list_5 + tedd_members_docs_list_5
# hua_members_docs_1 = dit_members_docs_list_1 + geo_members_docs_list_1 + tova_members_docs_list_1 + tedd_members_docs_list_1

# print("Δείκτης Μ1.265 (Εργασίες με κριτές scopus σωρευτικά τα τελευταία 5 έτη) - ΧΑΡΟΚΟΠΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ: ",len(set(hua_members_docs_5)))
# print("Δείκτης Μ1.266 (Εργασίες με κριτές scopus το έτος αναφοράς) - ΧΑΡΟΚΟΠΕΙΟ ΠΑΝΕΠΙΣΤΗΜΙΟ: ",len(set(hua_members_docs_1)))

# df_hua_members_docs_5 = pd.DataFrame(hua_members_docs_5)
# df_hua_members_docs_5.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/all_hua_members_docs_5" + ".csv",sep=';')
# df_hua_members_docs_5_dedub = pd.DataFrame(set(hua_members_docs_5))
# df_hua_members_docs_5_dedub.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/all_hua_members_docs_5_dedub" + ".csv",sep=';')

# df_hua_members_docs_1 = pd.DataFrame(hua_members_docs_1)
# df_hua_members_docs_1.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/all_hua_members_docs_1" + ".csv",sep=';')
# df_hua_members_docs_1_dedub = pd.DataFrame(set(hua_members_docs_1))
# df_hua_members_docs_1_dedub.to_csv("/home/pboump/PROJECTS/Python/scop/elsapy/files/all_hua_members_docs_1_dedub" + ".csv",sep=';')

# #################################################################################################################

# count_author_docs("56252345700",year_range=1)
# count_multiple_authors_docs()


# anafores,eteroanafores = count_multiple_citations("dept_tova_ID.csv",1)
# print("Δείκτης Μ1.270 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΟΒΑ: ",anafores)
# print("Δείκτης Μ1.233 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΟΒΑ: ",eteroanafores)

# anafores,eteroanafores = count_multiple_citations("dept_tova_ID.csv",5)
# print("Δείκτης Μ1.269 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΟΒΑ: ",anafores)
# print("Δείκτης Μ1.268 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΟΒΑ: ",eteroanafores)

# anafores,eteroanafores = count_multiple_citations("dept_tedd_ID.csv",1)
# print("Δείκτης Μ1.270 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΕΔΔ: ",anafores)
# print("Δείκτης Μ1.233 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΕΔΔ: ",eteroanafores)

anafores,eteroanafores = count_multiple_citations("dept_tedd_ID.csv",5)
print("Δείκτης Μ1.269 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΕΔΔ: ",anafores)
print("Δείκτης Μ1.268 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΕΔΔ: ",eteroanafores)

# anafores,eteroanafores = count_multiple_citations("dept_geo_ID.csv",1)
# print("Δείκτης Μ1.270 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΓΕΩ: ",anafores)
# print("Δείκτης Μ1.233 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΓΕΩ: ",eteroanafores)

# anafores,eteroanafores = count_multiple_citations("dept_geo_ID.csv",5)
# print("Δείκτης Μ1.269 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΓΕΩ: ",anafores)
# print("Δείκτης Μ1.268 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΓΕΩ: ",eteroanafores)

# anafores,eteroanafores = count_multiple_citations("dept_dit_ID.csv",1)
# print("Δείκτης Μ1.270 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΠΤ: ",anafores)
# print("Δείκτης Μ1.233 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus στο έτος αναφοράς (31/12).) - ΤΠΤ: ",eteroanafores)

# anafores,eteroanafores = count_multiple_citations("dept_dit_ID.csv",5)
# print("Δείκτης Μ1.269 (Σύνολο αναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΠΤ: ",anafores)
# print("Δείκτης Μ1.268 (Σύνολο ετεραναφορών των δημοσιεύσεων των μελών στο Scopus σωρευτικά για τα 5 τελευταία έτη.) - ΤΠΤ: ",eteroanafores)
