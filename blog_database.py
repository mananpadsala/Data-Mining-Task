# Importing Libraries
import os
from multiprocessing import Pool #Multiprocessing library
import pandas as pd
from bs4 import BeautifulSoup #WebScraping Library
import requests #URLRequest Library


def blog_database(list):
    start = list[0]
    end = list[1]
    print('From {} to {}:'.format(list[0], list[1]))
    # Defining of the dataframe
    df = pd.DataFrame(columns=['Index', 'Title', 'Blog_Link', 'Category',
                      'Category_Link', 'Publisher', 'Publisher_Link', 'Date', 'Description'])
    # Preparing Soup
    for page in range(start, end+1):
        response = requests.get(f'https://blog.getlatka.com/page/{page}/').text
        soup = BeautifulSoup(response,features="html5lib")

        #main scraping starts
        Title = []
        for title in soup.find_all('div', class_="jeg_postblock_content"):
            Title.append(title.a.text if title.a else "NaN")
        Blog_Link = []
        for blog_link in soup.find_all('div', class_="jeg_postblock_content"):
            Blog_Link.append(blog_link.a.get("href") if blog_link.a else "NaN")
        Category = []
        for category in soup.find_all('div', class_="jeg_meta_category"):
            Category.append(category.a.text if category.a else "NaN")
        Category_Link = []
        for category_link in soup.find_all('div', class_="jeg_meta_category"):
            Category_Link.append(category_link.a.get(
                "href") if category_link.a else "NaN")
        Publisher = []
        for publisher in soup.find_all('div', class_="jeg_meta_author"):
            Publisher.append(publisher.a.text if publisher.a else "NaN")
        Publisher_Link = []
        for publisher_link in soup.find_all('div', class_="jeg_meta_author"):
            Publisher_Link.append(publisher_link.a.get(
                "href") if publisher_link.a else "NaN")
        Date = []
        for date in soup.find_all('div', class_="jeg_meta_date"):
            Date.append(date.text if date else "NaN")
        Description = []
        for description in soup.find_all('div', class_="jeg_post_excerpt"):
            Description.append(description.p.text if description.p else "NaN")
        Title = Title[:10]
        Blog_Link = Blog_Link[:10]
        Category = Category[:10]
        Category_Link = Category_Link[:10]
        Publisher = Publisher[:10]
        Publisher_Link = Publisher_Link[:10]
        Date = Date[:10]
        Description = Description[:10]
        for i in range(10):
            df = df.append({'Index': i, 'Title': Title[i] if len(Title) == 10 else "NaN", 'Blog_Link': Blog_Link[i] if len(Blog_Link) == 10 else "NaN", 'Category': Category[i] if len(Category) == 10 else "NaN", 'Category_Link': Category_Link[i] if len(Category_Link) == 10 else "NaN", 'Publisher': Publisher[i] if len(
                Publisher) == 10 else "NaN", 'Publisher_Link': Publisher_Link[i] if len(Publisher_Link) == 10 else "NaN", 'Date': Date[i] if len(Date) == 10 else "NaN", 'Description': Description[i] if len(Description) == 10 else "NaN"}, ignore_index=True)
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
    pool.map(blog_database, list_ranges)


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
    df.drop(['Unnamed: 0','Index'], axis=1, inplace=True)
    df.to_json("blog_data.json") #Saving in JSON Fromat
    delete_csv_files(n,parts)

def delete_csv_files(n,parts):
    for f in merge_list_creator(n, parts):
        os.remove(os.path.join(os.getcwd(), f))


# Driver code
if __name__ == '__main__':
    run_parallel(50, 10)     #Here I've scraped 50 pages dividing into 10 sub processes 
    combine_csv(50, 10)

