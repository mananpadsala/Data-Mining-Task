# Importing Libraries
import os
from multiprocessing import Pool #Multiprocessing library
import pandas as pd
from bs4 import BeautifulSoup #WebScraping Library
import requests #URLRequest Library

#Main Scraping Function (It is scraping the main SaaS Companies Dataset)
#Eg: saas_database([[1-50],[51-100],[101-150],[151-200]])
#list contains the sublists containing the start and end page no. for parallel scraping
#This way we will be pass the parts for scraping 200 pages
def saas_database(list):
    start = list[0]
    end = list[1]
    print('From {} to {}:'.format(list[0], list[1]))  #Will show subprocesses (Eg From 1 to 50 pages)
    # Defining dataframe
    df = pd.DataFrame(columns=['Name', 'Company_Information_link', 'Company_LinkedIn', 'Company_Crunchbase', 'Revenue', 'Funding',
                      'Valuation', 'Cash_Flow', 'Founder', 'Founder_LinkedIn', 'Team_Size', 'Age', 'Location', 'Industry', 'As_Of'])
    # Preparing Soup
    for page in range(start, end+1):
        response = requests.get(
            f'https://getlatka.com/saas-companies?page={page}').text
        soup = BeautifulSoup(response,features="html5lib")
        for table in soup.find_all('table'):
            page = page
        
        for row in table.tbody.find_all('tr'):  # Traversing row by row in the table
            columns = row.find_all('td')  # traversing along columns in a particular row

            #main scraping starts
            if (columns != []):
                Name = columns[1].a.text.strip()
                Company_LinkedIn = columns[1].find('a', {"aria-label": "Company LinkedIn"}).get(
                    "href") if columns[1].find('a', {"aria-label": "Company LinkedIn"}) else 'NaN'
                Company_Crunchbase = columns[1].find('a', {"aria-label": "Company Crunchbase"}).get(
                    "href") if columns[1].find('a', {"aria-label": "Company Crunchbase"}) else 'NaN'
                Revenue = columns[2].text.strip()
                Funding = columns[3].text.strip()
                Valuation = columns[4].text.strip()
                Cash_Flow = columns[5].text.strip()
                Founder = columns[6].text.strip()
                Founder_LinkedIn = columns[6].find('a', {"aria-label": "founder-linkedin"}).get(
                    "href") if columns[6].find('a', {"aria-label": "founder-linkedin"}) else 'NaN'
                Team_Size = columns[7].text.strip()
                Age = columns[8].text.strip()
                Location = columns[9].text.strip()
                Industry = columns[10].text.strip()
                As_Of = columns[11].text.strip()
                Company_Information_link = 'https://getlatka.com/' + \
                    (columns[1].a).get("href")
                df = df.append({'Name': Name, 'Company_LinkedIn': Company_LinkedIn, 'Company_Crunchbase': Company_Crunchbase, 'Revenue': Revenue, 'Funding': Funding, 'Company_Information_link': Company_Information_link,
                               'Founder_LinkedIn': Founder_LinkedIn, 'Age': Age, 'Cash_Flow': Cash_Flow, 'Team_Size': Team_Size, 'Location': Location, 'Industry': Industry, 'As_Of': As_Of, 'Valuation': Valuation, 'Founder': Founder}, ignore_index=True)
    df.to_csv(f"{start}-{end}.csv")  #saving all the files generated from each subprocess


#function to produce list of ranges for parallel processes
def start_end(n, parts):
    lists = []
    ct = 0
    i = 1
    while (ct < parts):
        lists.append([i, i+int(n/parts)-1])
        i += int(n/parts)
        ct = ct+1
    return lists


#Parallel Processing Function
def run_parallel(n, parts):

    # list of ranges to execute for each parallel process
    list_ranges = start_end(n, parts)  #start_end function defined above

    # pool object with number of elements in the list
    pool = Pool(processes=len(list_ranges))

    # map the function to the list and pass
    # function and list_ranges as arguments
    pool.map(saas_database, list_ranges)


#function to create lists of all .csv files to be merged
def merge_list_creator(n, parts):
    lists = []
    ct = 0
    i = 1
    while (ct < parts):
        lists.append(str(int(i))+'-' + str(i+int(n/parts)-1) + '.csv')
        i += int(n/parts)
        ct = ct+1
    return lists




# Combine all csv file created by parallel process
def combine_csv(n, parts):
    import pandas as pd
    # merging all the csv files
    df = pd.concat(
        map(pd.read_csv, merge_list_creator(n, parts)), ignore_index=True)
    df.drop('Unnamed: 0', axis=1, inplace=True)
    df.to_json("saas_data.json") #Saving in JSON Fromat
    delete_csv_files(n,parts)

def delete_csv_files(n,parts):
    for f in merge_list_creator(n, parts):
        os.remove(os.path.join(os.getcwd(), f))


# Driver code
if __name__ == '__main__':
    run_parallel(800, 16)     #Here I've scraped 800 pages dividing into 16 sub processes 
    combine_csv(800, 16)
