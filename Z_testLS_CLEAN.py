import re
import pandas as pd
import configparser
from sqlalchemy import (
    create_engine,
    update,
    MetaData,
    text,
    select,
    insert,
    delete
)
import logging
import numpy as np
import json
import time
from bs4 import BeautifulSoup
import requests
import os
import time
from datetime import datetime
current_date = datetime.today().date()
import argparse
from requests.exceptions import RequestException
config = configparser.ConfigParser(interpolation=None)
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))
db_prefix =  config.get("mysql","db_prefix")
DB_NAME = config.get("mysql","database")

file_name = config.get("file","filename")
parser = argparse.ArgumentParser()
parser.add_argument("--filename", default=file_name, type=str, help="xlsx file for processing")
# Parse the arguments
args = parser.parse_args()
file_name = args.filename if args.filename else file_name


OUTPUT_FILE = f"{current_date}{DB_NAME}.csv" #sould be a CSV file
logging.basicConfig(filename=f"LinuxSecurity-HeaderTitle-Logs-{current_date}.txt ", level=logging.DEBUG)


conn_params = {
    "user": config.get("mysql","user"),
    "password": config.get("mysql","password"),
    "host": config.get("mysql","host"),
    "port": config.get("mysql","port"),
    "database": config.get("mysql","database"),
}
# Establish a connection
engine = create_engine(
    "mysql+pymysql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s"
    % conn_params,
    future=True,
    pool_pre_ping=True,
    pool_recycle=3600,
)
conn = engine.connect()

formatter = logging.Formatter(
    fmt="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

start_time = time.time()


# infodf = pd.read_excel('Meta_H1_Info-121123.xlsx')
infodf = pd.read_excel(file_name, "New Website Plan Draft",)
mapdf = pd.read_excel(file_name, "Meta Data Mapping")


infodf.columns = infodf.iloc[0]
infodf = infodf.iloc[1:]
# Create a DataFrame with NaN values for the new rows
new_rows_df = pd.DataFrame(columns=infodf.columns, index=range(3))
new_rows_df[:] = np.nan

# Concatenate the new rows DataFrame with the existing DataFrame
infodf = pd.concat([new_rows_df, infodf], ignore_index=True)
infodf["map_id"] = infodf.index
infodf["map_data"] = ""
infodf["meta_tag"] = False
infodf["H1_tag"] = False





meta = MetaData()
meta.reflect(bind=engine)
menu_table = pd.read_sql_query(
    text(f"SELECT * FROM {db_prefix}menu where type <> 'url'"), conn
)
content_table = pd.read_sql_query(text(f"SELECT * FROM {db_prefix}content "), conn)
# edocman_categories_table = pd.read_sql_query(
#     text(f"SELECT * FROM {db_prefix}edocman_categories"), conn
# )
categories_table = pd.read_sql_query(text(f"SELECT * FROM {db_prefix}categories"), conn)
# casestudies_table = pd.read_sql_query(text(f"SELECT * FROM {db_prefix}casestudies_iq"), conn)
# modules_table = pd.read_sql_query(
#     text(f"SELECT * FROM {db_prefix}modules where module='mod_header_iq'"), conn
# )
# fields_values_table = pd.read_sql_query(text(f"SELECT * FROM {db_prefix}fields_values"), conn)

# contentitem_tag_map_table = pd.read_sql_query(text(f"SELECT * FROM {db_prefix}contentitem_tag_map" ), conn)
# tags_table = pd.read_sql_query(text(f"SELECT * FROM {db_prefix}tags"), conn)



infodf["Inspiration / Current URL (if existing page)"] = infodf[
    "Inspiration / Current URL (if existing page)"
].fillna("")
infodf["Old H1"] = infodf["Old H1"].fillna("")
infodf["New Header "] = infodf["New Header"].fillna("")
infodf["Old Title"] = infodf["Old Title"].fillna("")
infodf["New Title"] = infodf["New Title"].fillna("")


infodf.replace({np.nan: ""}, inplace=True)

# Handling null
infodf["Old Metas"] = infodf["Old Metas"].fillna("")
infodf["URL"] = infodf["URL"].fillna("")

infodf["Old Metas"] = infodf["Old Metas"].apply(
    lambda x: x.replace("\\n", "\n").replace("\\r", "\r")
)

infodf["Extracted Text"] = infodf["Inspiration / Current URL (if existing page)"].apply(
    lambda url: url.split("/")[-1].split('?')[0]
)
map_dic = {}
for i, vals in mapdf.iterrows():
    if "-" in str(vals["Rows"]):
        if vals["Rows"] == "-":
            continue
        strat = int(vals["Rows"].split("-")[0])
        end = int(vals["Rows"].split("-")[1])
        for num in range(strat, end + 1):
            update_prefix_h1 = db_prefix + vals["H1 Location (Table - Column)"].split("_", 1)[1]
            meta_des_location = db_prefix + vals["Meta Desc Location (Table - Column)"].split("_", 1)[1]
            map_dic[num] = [
                vals["Type"],
                update_prefix_h1,
                meta_des_location
            ]
    else:
        prefix_name_db = vals["H1 Location (Table - Column)"]
        meta_des_location = vals["Meta Desc Location (Table - Column)"]
        if "_" in prefix_name_db:
            update_prefix_h1 = db_prefix + vals["H1 Location (Table - Column)"].split("_", 1)[1]
            meta_des_location = db_prefix + vals["Meta Desc Location (Table - Column)"].split("_", 1)[1]
        else:
            update_prefix_h1  = prefix_name_db
            meta_des_location = db_prefix + vals["Meta Desc Location (Table - Column)"].split("_", 1)[1]

    
        map_dic[vals["Rows"]] = [
            vals["Type"],
            update_prefix_h1,
            meta_des_location,
        ]
logging.info(f"Total records found in mapping {len(map_dic)}")
# Initialize a list to store the link values
Meta_link_values = []
H1_link_values = []
Article_link_values = []
Title_link_values = []
Page_types = []

# def sp_blog_top_data_module_id(url):
#     response = requests.get(url)
#     html_content = response.text
#     soup = BeautifulSoup(html_content, 'html.parser')
#     div = soup.find('div', id='sp-blog-top')
#     if div:
#         elements = div.find_all(attrs={"data-module_id": True})
#         # Print the values of 'data-module_id' for all found elements
#         for el in elements:
#             data = el['data-module_id']
#             return data
   


def sp_blog_top_data_module_id(url, max_retries=3, delay=1):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise HTTPError for bad status codes
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            div = soup.find('div', id='sp-blog-top')
            if div:
                elements = div.find_all(attrs={"data-module_id": True})
                # Print the values of 'data-module_id' for all found elements
                for el in elements:
                    data = el['data-module_id']
                    return data
            else:
                print("No 'div' element with id 'sp-blog-top' found.")
                return None
        except RequestException as e:
            print(f"Request failed: {e}")
            retries += 1
            if retries < max_retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. Exiting...")
    return None



def get_title_sp_page_builder(id,new_title):
   query = f"SELECT * FROM {db_prefix}sppagebuilder WHERE id = :id"  # Using parameterized query to avoid SQL injection
   sppagebuilder_table = pd.read_sql_query(text(query), conn, params={'id': id})
   text_field  = sppagebuilder_table['text'].iloc[0]
   all_text  = json.loads(text_field)
   for item in all_text:
      # Check if the element is a dictionary
      if isinstance(item, dict):
         # Retrieve the 'columns' key from the dictionary
         columns = item.get("columns", [])
         for column in columns:
               addons = column.get("addons", [])
               # Iterate over 'addons'
               for addon in addons:
                  # Check if 'settings' key exists in addon
                  settings = addon.get("settings", {})
                  heading_selector = settings.get("heading_selector")
                  if heading_selector =="h1":
                     settings['title'] = new_title
                     # Retrieve the 'title' key from 'settings' dictionary and print it
                     new_title = settings.get("title")
                     print(f"Updated title to: {new_title}")

   updated_text = json.dumps(all_text)
   update_query = f"UPDATE {db_prefix}sppagebuilder SET text = :new_text WHERE id = :id"
   conn.execute(text(update_query), {'new_text': updated_text, 'id': id})
   conn.commit()
   return updated_text




module_id_map =  {'https://guardiandigital.com/testimonials': '194', 'https://guardiandigital.com/customer-success-stories/aalborg-instruments': '489', 'https://guardiandigital.com/customer-success-stories/att-new-zealand': '490', 'https://guardiandigital.com/customer-success-stories/bc-media-gains-peace-of-mind-closes-microsoft-365-security-gaps-with-guardian-digital': '491', 'https://guardiandigital.com/customer-success-stories/casestudies': '492', 'https://guardiandigital.com/customer-success-stories/chicago-stock-exchange': '493', 'https://guardiandigital.com/customer-success-stories/community-home-entertainment': '494', 'https://guardiandigital.com/customer-success-stories/deepinthemoney': '495', 'https://guardiandigital.com/customer-success-stories/digitize-inc': '496', 'https://guardiandigital.com/customer-success-stories/global-500-travel-division': '497', 'https://guardiandigital.com/customer-success-stories/government-of-india-indian-space-research-organization': '499', 'https://guardiandigital.com/customer-success-stories/intrepid-group': '498', 'https://guardiandigital.com/customer-success-stories/itasca-independent-school-system': '500', 'https://guardiandigital.com/customer-success-stories/jerry-gaffney-ministries': '501', 'https://guardiandigital.com/customer-success-stories/jersey-shore-federal-credit-union': '502', 'https://guardiandigital.com/customer-success-stories/ny-spine-care': '503', 'https://guardiandigital.com/customer-success-stories/piedmont-natural-gas': '504', 'https://guardiandigital.com/customer-success-stories/secure-automotive-sales': '505', 'https://guardiandigital.com/customer-success-stories/wavewizard-internet': '506', 'https://guardiandigital.com/customer-success-stories/worlds-largest-hotel-family': '507', 'https://guardiandigital.com/resources/media-center': '168', 'https://guardiandigital.com/resources/press-releases': '484', 'https://guardiandigital.com/resources/press-coverage': '192', 'https://guardiandigital.com/resources/faq': '339', 'https://guardiandigital.com/resources/faq/can-i-get-ransomware-by-opening-email': '400', 'https://guardiandigital.com/resources/faq/can-you-get-virus-from-opening-email': '400', 'https://guardiandigital.com/resources/faq/clicked-on-phishing-link': '386', 'https://guardiandigital.com/resources/faq/difference-between-spam-phishing': '388', 'https://guardiandigital.com/resources/faq/examples-of-malicious-code': '433', 'https://guardiandigital.com/resources/faq/how-can-i-protect-against-ransomware': '384', 'https://guardiandigital.com/resources/faq/how-can-i-recognize-a-fraudulent-email': '399', 'https://guardiandigital.com/resources/faq/how-improve-office-365-security': '397', 'https://guardiandigital.com/resources/faq/how-to-recognize-spam-emails': '398', 'https://guardiandigital.com/resources/faq/how-to-scan-windows-pc-for-malware-remove-malware': '434', 'https://guardiandigital.com/resources/faq/how-to-spot-a-phishing-email': '399', 'https://guardiandigital.com/resources/faq/is-g-suite-secure-for-business-email': '387', 'https://guardiandigital.com/resources/faq/url-defense': '389', 'https://guardiandigital.com/resources/faq/what-are-denial-of-service-dos-attacks': '480', 'https://guardiandigital.com/resources/faq/what-does-bec-stand-for': '395', 'https://guardiandigital.com/resources/faq/what-is-a-man-in-the-middle-attack': '390', 'https://guardiandigital.com/resources/faq/what-is-advantage-of-encrypting-email': '401', 'https://guardiandigital.com/resources/faq/what-is-clone-phishing': '392', 'https://guardiandigital.com/resources/faq/what-is-guardian-digital-engarde-cloud-email-security': '432', 'https://guardiandigital.com/resources/faq/what-is-ryuk-ransomware': '391', 'https://guardiandigital.com/resources/faq/why-do-we-need-email-security': '402', 'https://guardiandigital.com/resources/faq/why-should-businesses-outsource-email-security': '481'}
# module_id_map = {}
for i, extracted_text in infodf.iterrows():
    print("current  row  processing: ",i)
    # print(f"Generating map data for {extracted_text['URL']}")
    Meta_link_value = ""
    H1_link_value = ""
    Article_link_value = ""
    Title_link_value = ""
    Page_Type_value = ""

    if extracted_text["map_id"] in map_dic:
        # if extracted_text["map_id"] == 345:
        #     print("heeee")
        # extracted_text['map_data'] = map_dic[extracted_text['map_id']]
        infodf.loc[i, "map_data"] = str(map_dic[extracted_text["map_id"]])
        if extracted_text["URL"] == "https://guardiandigital.com/":
            infodf.loc[i, "Extracted Text"] = "home"
            extracted_text["Extracted Text"] = "home"
            extracted_text["Inspiration / Current URL (if existing page)"] = (
                "https://guardiandigital.com/home"
            )

        if ".jpg" in extracted_text["URL"]:
            match = re.search(r"/([^/]+)\?", extracted_text["URL"])
            updated_url = match.group(1)
            infodf.loc[i, "Extracted Text"] = updated_url
            extracted_text["Extracted Text"] = updated_url
            extracted_text["Inspiration / Current URL (if existing page)"] = (
                "https://guardiandigital.com/" + updated_url
            )
        if f"{db_prefix}menu" in map_dic[extracted_text["map_id"]][2]:
            try:
                if extracted_text["Extracted Text"] in menu_table["alias"].values:
                    # link_value = menu_table.loc[menu_table['alias'] == extracted_text['Extracted Text'], 'link'].values[0]
                    infodf.loc[i, "meta_tag"] = True
                    # Meta_link_value = map_dic[extracted_text['map_id']][2].split(' - ')[0]

                    Meta_link_h1 = menu_table.loc[
                        (menu_table["alias"] == extracted_text["Extracted Text"])
                        & (
                            menu_table["path"]
                            == extracted_text[
                                "Inspiration / Current URL (if existing page)"
                            ].split(".com/")[-1]
                        ),
                        "link",
                    ].values[0]
                    # metaMetaTable = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'params'].values[0]
                    Meta_link_id = menu_table.loc[
                        (menu_table["alias"] == extracted_text["Extracted Text"])
                        & (
                            menu_table["path"]
                            == extracted_text[
                                "Inspiration / Current URL (if existing page)"
                            ].split(".com/")[-1]
                        ),
                        "id",
                    ].values[0]
                    # print(metaMetaTable)
                    Title_link_value = Meta_link_value = (
                        f"index.php?option=com_menu&view=metadescription&id={Meta_link_id}"
                    )
                    h1_table = (
                        map_dic[extracted_text["map_id"]][1]
                        .split("_")[1]
                        .split(" - ")[0]
                    )
                    if (
                        h1_table in Meta_link_h1
                        and h1_table == "sppagebuilder"
                        and extracted_text["Extracted Text"] != "home"
                    ):
                        infodf.loc[i, "H1_tag"] = True
                        H1_link_value = Meta_link_h1
                elif (
                    extracted_text["Extracted Text"] in content_table["alias"].values
                    and 345 <= extracted_text["map_id"] <= 366
                ):
                    infodf.loc[i, "meta_tag"] = True
                    id_content = content_table.loc[
                        content_table["alias"] == extracted_text["Extracted Text"], "id"
                    ].values[0]
                    Title_link_value = Meta_link_value = (
                        f"index.php?option=com_content&view=article&id={id_content}"
                    )

            except Exception as e:
                logging.info(f"Error getting Value for {extracted_text['URL'] }: {e}")
                Title_link_value = Meta_link_value = ""
        if f"{db_prefix}menu" in map_dic[extracted_text["map_id"]][2]:
            try:
                if extracted_text["Extracted Text"] in menu_table["alias"].values:
                    # link_value = menu_table.loc[menu_table['alias'] == extracted_text['Extracted Text'], 'link'].values[0]
                    Title_link_value_id = menu_table.loc[
                        (menu_table["alias"] == extracted_text["Extracted Text"])
                        & (
                            menu_table["path"]
                            == extracted_text[
                                "Inspiration / Current URL (if existing page)"
                            ].split(".com/")[-1]
                        ),
                        "id",
                    ].values[0]
                    # print(metaMetaTable)
                    Title_link_value = f"index.php?option=com_menu&view=item&client_id=0&layout=edit&id={Title_link_value_id}"

            except Exception as e:
                logging.info(f"Title extraction error:{e} ")
                Title_link_value = ""



  
        # elif (
        #     f"{db_prefix}content" in map_dic[extracted_text["map_id"]][2]
        #     and extracted_text["Extracted Text"] in content_table["alias"].values
        # ):
        #     infodf.loc[i, "meta_tag"] = True
        #     try:
        #         id_content = content_table.loc[
        #             content_table["alias"] == extracted_text["Extracted Text"], "id"
        #         ].values[0]
        #         Title_link_value = Meta_link_value = (
        #             f"index.php?option=com_content&view=article&id={id_content}"
        #         )
        #         Article_link_value = (
        #             f"index.php?option=com_content&view=article&id={id_content}"
        #         )

        #     except:
        #         Title_link_value = Meta_link_value = ""
        #         Article_link_value = ""
        # elif (
        #     f"{db_prefix}edocman_categories" in map_dic[extracted_text["map_id"]][2]
        #     and extracted_text["Extracted Text"]
        #     in edocman_categories_table["alias"].values
        # ):
        #     infodf.loc[i, "meta_tag"] = True
        #     try:
        #         id_category = edocman_categories_table.loc[
        #             edocman_categories_table["alias"]
        #             == extracted_text["Extracted Text"],
        #             "id",
        #         ].values[0]
        #         Title_link_value = Meta_link_value = (
        #             f"index.php?option=com_edocman_categories&view=categories&id={id_category}"
        #         )
        #     except:
        #         Title_link_value = Meta_link_value = ""
        # elif (
        #     f"{db_prefix}casestudies_iq" in map_dic[extracted_text["map_id"]][1]
        #     and extracted_text["Extracted Text"] in casestudies_table["alias"].values
        # ):
        #     infodf.loc[i, "meta_tag"] = True
        #     try:
        #         id_casestudios = casestudies_table.loc[
        #             casestudies_table["alias"] == extracted_text["Extracted Text"], "id"
        #         ].values[0]
        #         Title_link_value = Meta_link_value = (
        #             f"index.php?option=com_casestudies_iq&view=casestudies&id={id_casestudios}"
        #         )
        #     except:
        #         Title_link_value = Meta_link_value = ""

        # else:
        #     infodf.loc[i, "meta_tag"] = False
        #     Title_link_value = Meta_link_value = ""

        # H1_link_value
        if (
            f"{db_prefix}menu" in map_dic[extracted_text["map_id"]][1]
            and extracted_text["Extracted Text"] in menu_table["alias"].values
        ):
            # link_value = menu_table.loc[menu_table['alias']   == extracted_text['Extracted Text'], 'link'].values[0]
            infodf.loc[i, "H1_tag"] = True
            try:
                # H1_link_value = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'link'].values[0]

                H1_link_value_id = menu_table.loc[
                    (menu_table["alias"] == extracted_text["Extracted Text"])
                    & (
                        menu_table["path"]
                        == extracted_text[
                            "Inspiration / Current URL (if existing page)"
                        ].split(".com/")[-1]
                    ),
                    "id",
                ].values[0]
                # print(metaMetaTable)
                H1_link_value = f"index.php?option=com_menu&view=metadescription&id={H1_link_value_id}"

            except Exception as e:

                H1_link_value = ""
                logging.error("Exception 1 occured")

        # elif (
        #     f"{db_prefix}content" in map_dic[extracted_text["map_id"]][1]
        #     and extracted_text["Extracted Text"] in content_table["alias"].values
        # ):
        #     infodf.loc[i, "H1_tag"] = True
        #     try:
        #         id_content = content_table.loc[
        #             content_table["alias"] == extracted_text["Extracted Text"], "id"
        #         ].values[0]
        #         H1_link_value = (
        #             f"index.php?option=com_content&view=article&id={id_content}"
        #         )

        #     except:
        #         H1_link_value = ""
        #         logging.error("Exception 2 occured")
        # elif (
        #     f"{db_prefix}edocman_categories" in map_dic[extracted_text["map_id"]][1]
        #     and extracted_text["Extracted Text"]
        #     in edocman_categories_table["alias"].values
        # ):
        #     infodf.loc[i, "H1_tag"] = True
        #     try:
        #         id_category = edocman_categories_table.loc[
        #             edocman_categories_table["alias"]
        #             == extracted_text["Extracted Text"],
        #             "id",
        #         ].values[0]
        #         # H1_link_value = f'index.php?option=com_categories&view=categories&id={id_category}'
        #         H1_link_value = f"index.php?option=com_edocman_categories&view=categories&id={id_category}"
        #     except:
        #         H1_link_value = ""
        #         logging.error("Exception 3 occured")
        # elif (
        #     f"{db_prefix}categories" in map_dic[extracted_text["map_id"]][1]
        #     and extracted_text["Extracted Text"] in categories_table["alias"].values
        # ):
        #     infodf.loc[i, "H1_tag"] = True
        #     try:
        #         id_category = categories_table.loc[
        #             categories_table["alias"] == extracted_text["Extracted Text"], "id"
        #         ].values[0]
        #         # H1_link_value = f'index.php?option=com_edocman_categories&view=categories&id={id_category}'
        #         H1_link_value = (
        #             f"index.php?option=com_categories&view=categories&id={id_category}"
        #         )
        #     except:
        #         H1_link_value = ""
        #         logging.error("Exception 4 occured")
      
        #SP Page Builder section
        ################################################################################################################
        elif (map_dic[extracted_text["map_id"]][0] == "SP Page Builder"):
            logging.info(
                f"map_id  is : {map_dic[extracted_text['map_id']][0]}"
            )

            if extracted_text["URL"] == "https://linuxsecurity.com/":
                infodf.loc[i, "Extracted Text"] = "home"
                extracted_text["Extracted Text"] = "home"
                extracted_text["Inspiration / Current URL (if existing page)"] = (
                    "https://linuxsecurity.com/home"
                )

            if extracted_text["Extracted Text"] in menu_table["alias"].values:
                try:
                    if extracted_text["Extracted Text"] in menu_table["alias"].values:
                        infodf.loc[i, "meta_tag"] = True
                        Meta_link_h1 = menu_table.loc[
                            (menu_table["alias"] == extracted_text["Extracted Text"])
                            & (
                                menu_table["path"]
                                == extracted_text[
                                    "Inspiration / Current URL (if existing page)"
                                ].split(".com/")[-1]
                            ),
                            "link",
                        ].values[0]
                        Meta_link_id = menu_table.loc[
                            (menu_table["alias"] == extracted_text["Extracted Text"])
                            & (
                                menu_table["path"]
                                == extracted_text[
                                    "Inspiration / Current URL (if existing page)"
                                ].split(".com/")[-1]
                            ),
                            "id",
                        ].values[0]
                        # print(metaMetaTable)
                        Title_link_value = Meta_link_value = (
                            f"index.php?option=com_menu&view=metadescription&id={Meta_link_id}"
                        )
                        # match = re.search(r'sppagebuilder',map_dic[extracted_text["map_id"]][1])
                        # if match:
                        #   ht_tables= match.group(0)
                       
                        # if (
                        #     h1_table in Meta_link_h1
                        #     and h1_table == "sppagebuilder"
                        #     and extracted_text["Extracted Text"] != "home"
                        # ):
                        if  extracted_text["Extracted Text"] != "home":
                            infodf.loc[i, "H1_tag"] = True
                            H1_link_value = Meta_link_h1
                            Page_Type_value = map_dic[extracted_text["map_id"]][0]

                    elif (
                        extracted_text["Extracted Text"] in content_table["alias"].values
                        and 345 <= extracted_text["map_id"] <= 366
                    ):
                        infodf.loc[i, "meta_tag"] = True
                        id_content = content_table.loc[
                            content_table["alias"] == extracted_text["Extracted Text"], "id"
                        ].values[0]
                        Title_link_value = Meta_link_value = (
                            f"index.php?option=com_content&view=article&id={id_content}"
                        )

                except Exception as e:
                    logging.info(f"Error getting Value for {extracted_text['URL'] }: {e}")
                    Title_link_value = Meta_link_value = ""


        
        elif (map_dic[extracted_text["map_id"]][0] == "SP Page Builder Module"):
            
        
            mod_id = None
            H1_link_value = ""
            if module_id_map.get(extracted_text["URL"]):
                mod_id=module_id_map.get(extracted_text["URL"])
            if mod_id:
                logging.info(f"Module id {mod_id} for URL: {extracted_text['URL']} found in Previous match")
                H1_link_value = f"index.php?option=com_modules&view=modules&id={mod_id}"
                # continue
            else:
                logging.info(f"Checking URL: {extracted_text['URL']} for module type record for H1")
                # html_data = requests.get(extracted_text["URL"])
                url = extracted_text["URL"]
                if not url =="":
                    view_id = sp_blog_top_data_module_id(url,max_retries=3, delay=2)
                    query = f"SELECT * FROM {db_prefix}sppagebuilder WHERE view_id = :view_id"  # Using parameterized query to avoid SQL injection
                    sppagebuilder_table = pd.read_sql_query(text(query), conn, params={'view_id': view_id})
                    id = sppagebuilder_table['id'].iloc[0]
                    print(i,"sppagebuilder_table id ",id , "View_id ",view_id,extracted_text["URL"])
                    H1_link_value = f"index.php?option=com_sppagebuilder&view=page&id={id}"
                    Page_Type_value = map_dic[extracted_text["map_id"]][0]
                    logging.info(
                        f"View_id: {view_id} Sppage id: {id}: URL {extracted_text['URL']}"   )
                else:
                    print(f"Row", i,  "Invaild url" )
                    logging.info(f"{i}, invaild url ")
                
            
                # if html_data.status_code != 200:
                #     logging.info(f"URL: {extracted_text['URL']} not found for Module type records, git statu_code {html_data.status_code}")
                #     # continue
                # html_data_text = html_data.text
                # pattern = re.compile(r'mod-\d+')
                # matched = re.search(pattern, html_data_text)
                # if matched:
                #     mod_id_class = html_data_text[matched.span()[0]: matched.span()[1]]
                #     mod_id = mod_id_class.split('-')[1]
                #     infodf.loc[i, "H1_tag"] = True
                #     module_id_map[extracted_text["URL"]] = mod_id
                #     H1_link_value = f"index.php?option=com_modules&view=modules&id={mod_id}"




        #####################################################################################################
        
            
        # elif (
        #     f"{db_prefix}modules" in map_dic[extracted_text["map_id"]][1]
        #     and modules_table["params"]
        #     .str.contains(re.escape(extracted_text["Old H1"]))
        #     .sum()
        #     > 0
        # ):
        #     infodf.loc[i, "H1_tag"] = True
        #     try:
        #         if (
        #             extracted_text["Old H1"]
        #             and modules_table["params"]
        #             .str.contains('"' + re.escape(extracted_text["Old H1"] + '"'))
        #             .sum()
        #             == 1
        #         ):
        #             id_menutable = modules_table.loc[
        #                 modules_table["params"].str.contains(
        #                     re.escape(extracted_text["Old H1"])
        #                 ),
        #                 "id",
        #             ].values[0]
        #             H1_link_value = (
        #                 f"index.php?option=com_modules&view=modules&id={id_menutable}"
        #             )
        #     except:
        #         H1_link_value = ""
        #         logging.error("Exception 6 occured")
    # meta_link_map[i]=f"{Meta_link_value} | {extracted_text['URL']}"    
    Meta_link_values.append(Meta_link_value)
    H1_link_values.append(H1_link_value)
    Article_link_values.append(Article_link_value)
    Title_link_values.append(Title_link_value)
    Page_types.append(Page_Type_value)

print("Module Id map \n",module_id_map)
print("Meta Description is updated")


# Add the link_values list as a new column in infodf
infodf["Meta_Link"] = Meta_link_values
infodf["H1_Link"] = H1_link_values
infodf["Article_Link"] = Article_link_values
infodf["Title_Link"] = Title_link_values
infodf["Page_type"] = Page_types

# Extract values for 'id' and 'com' columns from the 'Link' column
infodf["H1_id"] = infodf["H1_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["H1_com"] = infodf["H1_Link"].apply(
    lambda link: (
        f"{db_prefix}" + link.split("com_")[1].split("&")[0] if "com_" in link else ""
    )
)
infodf["Meta_id"] = infodf["Meta_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["Meta_com"] = infodf["Meta_Link"].apply(
    lambda link: (
        f"{db_prefix}" + link.split("com_")[1].split("&")[0] if "com_" in link else ""
    )
)
infodf["Article_link"] = infodf["Article_Link"].apply(
    lambda link: (
        f"{db_prefix}" + link.split("com_")[1].split("&")[0]
        if "com_" and "content" in link
        else ""
    )
)
infodf["Article_id"] = infodf["Article_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["Title_id"] = infodf["Title_Link"].apply(
    lambda link: link.split("=")[-1] if "=" in link else ""
)
infodf["Title_com"] = infodf["Title_Link"].apply(
    lambda link: (
        f"{db_prefix}" + link.split("com_")[1].split("&")[0] if "com_" in link else ""
    )
)

article_ids = []
title_ids = [title_id for title_id in infodf["Title_id"]]
for article_id in infodf["Article_id"]:
    article_ids.append(article_id)


infodf["H1_Updated_DB"] = False
infodf["Meta_Updated_DB"] = False
infodf["Title_Updated_DB"] = False
infodf["Fields_Tables_D"] = False

# infodf.to_csv("file_name55.csv")
# print(infodf)
# exit(1)
# Create a list to store the queries and results
query_results = []

updated_values = set()

w5zxq_modules = meta.tables[f"{db_prefix}modules"]


logging.info(f"\t\t\t\t\t\tOLD\t\t\t\t\t\t\t\t\tNew")


# def update_fields_values_table()
fields = [
    "row_number",
    "record_url",
    "h1_table_name",
    "h1_record_id",
    "new_h1_value",
    "title_table_name",
    "title_record_id",
    "new_title_value",
    "description_table_name",
    "description_record_id",
    "new_description_value",
]
table_rows = []
total_records_by_type = {}
empty_records = {}
no_diff_records = {}
sppagebuilder_count = 0
for index, (
    h1_id_value,
    h1_com_value,
    meta_id_value,
    meta_com_value,
    map_id,
    map_data,
    meta_tag,
    H1_tag,
    new_H1,
    old_h1,
    new_title,
    old_title,
    old_Meta,
    New_Meta,
    title_com_value,
    title_id_value,
    record_url,
    page_type,
) in enumerate(
    zip(
        infodf["H1_id"],
        infodf["H1_com"],
        infodf["Meta_id"],
        infodf["Meta_com"],
        infodf["map_id"],
        infodf["map_data"],
        infodf["meta_tag"],
        infodf["H1_tag"],
        infodf["New Header"],
        infodf["Old H1"],
        infodf["New Title"],
        infodf["Old Title"],
        infodf["Old Metas"],
        infodf["New Meta Description"],
        infodf["Title_com"],
        infodf["Title_id"],
        infodf["URL"],
        infodf["Page_type"],
    )
):
#     table_rows.append(
#         {
#             "row_number": index,
#             "record_url": record_url,
#             "h1_table_name": h1_com_value,
#             "h1_record_id": h1_id_value,
#             "new_h1_value": new_H1,
#             "title_table_name":title_com_value,
#             "title_record_id": title_id_value,
#             "new_title_value": new_title,
#             "description_table_name": meta_com_value,
#             "description_record_id": meta_id_value,
#             "new_description_value": New_Meta,
#         }
#     )
#     print(f"added row {index}")
# #     # continue
# import csv
# with open("processed9.csv", 'w') as csvfile:
#     writer = csv.DictWriter(csvfile, fieldnames=fields)
#     writer.writeheader()
#     writer.writerows(table_rows)
# print("all records added in csv.")

#     logging.info(
#         f"Row {index}:| h1 table: {h1_id_value},record_url: {record_url}, starting process"
#     )
    try:

        if h1_com_value and h1_id_value.isdigit():
            if (
                h1_com_value != f"{db_prefix}rsform"
                and h1_com_value != f"{db_prefix}blockcontent"
                and h1_com_value != f"{db_prefix}edocman"
            ):
                if not total_records_by_type.get(h1_com_value):
                    total_records_by_type[h1_com_value] = [f"{index} : {h1_id_value}"]
                else:
                    total_records_by_type[h1_com_value].append(
                        f"{index} : {h1_id_value}"
                    )
                # if com_value in ('{db_prefix}sppagebuilder' , '{db_prefix}content'):
                # Construct the SQL query with the table name from 'com' column
                tableComValue = meta.tables[h1_com_value]
                print(f"Processing Row {index}: table {tableComValue}")
                # query = select(tableComValue).where(tableComValue.c.id == h1_id_value)
                query = select(tableComValue).where(
                    tableComValue.c.id == h1_id_value
                )  # # change back to above condition

                # print(query)
                try:
                    # Execute the query to check if the table exists
                    df = pd.read_sql_query(query, conn)
                except pd.io.sql.DatabaseError as e:
                    logging.info(
                        f"Row {index}: Error executing query {meta_query}: {e}"
                    )
                    # print(f"Error executing query for table '{com_value}': {e}")
                    continue

                # Check if the table exists
                if df.empty:
                    logging.info(f"Row {index}: Empty Dataframe for current row")
                    # print(f"Table '{com_value}' doesn't exist. Skipping...")
                    continue

                if map_id in (401, 403, 406):
                    logging.info(f"Row {index}: skipping map id")
                    print(map_id)

                for i, row in df.iterrows():
                    update_stmt = ""
                    # 1 sppage_builder --> replace of H1

                    if "sppagebuilder" in h1_com_value:
                        get_title_sp_page_builder(h1_id_value,new_H1)
                        
                    # 2 _content --> replace of title
                    # elif f"{db_prefix}menu" in h1_com_value:  #
                    #     tablemenu = meta.tables[f"{db_prefix}menu"]
                    #     params = row["params"]
                    #     json_params = json.loads(params)
                    #     if json_params.get("page_title", "") != new_H1:
                    #         json_params["page_title"] = new_H1
                    #         updated_string = json.dumps(json_params)
                    #         update_stmt = (
                    #             update(tablemenu)
                    #             .where(tablemenu.c.id == int(h1_id_value))
                    #             .values(params=updated_string)
                    #         )
                    #         with engine.begin() as connection:
                    #             connection.execute(update_stmt)
                    #             infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "H1_Updated_DB",
                    #         ] = True
                    #         logging.info(
                    #             f'Row {index}: {db_prefix}menu: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                    #         )
                    #     else:
                    #         if not no_diff_records.get(h1_com_value):
                    #             no_diff_records[h1_com_value] = [
                    #                 f"{index} : {h1_id_value}"
                    #             ]
                    #         else:
                    #             no_diff_records[h1_com_value].append(
                    #                 f"{index} : {h1_id_value}"
                    #             )
                    #         logging.info(
                    #             f"Row {index} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : old and new h1 tags are same."
                    #         )
                    # elif "_content" in h1_com_value:  # Done for both H1 and Title
                    #     tableContent = meta.tables[f"{db_prefix}content"]
                    #     max_spacing = 90  # Maximum spacing between columns
                    #     spacing = max_spacing - len(old_h1)
                    #     logging.info(
                    #         f'Row {index}: Content	Table:{h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                    #     )
                    #     # Construct the SQL update statement
                    #     update_stmt_h1 = (
                    #         update(tableContent)
                    #         .where(tableContent.c.id == int(h1_id_value))
                    #         .values(title=new_H1)
                    #     )
                    #     with engine.begin() as connection:
                    #         connection.execute(update_stmt_h1)
                    #         infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "H1_Updated_DB",
                    #         ] = True

                    #     newMeta = New_Meta
                    #     update_stmt_meta = (
                    #         update(tableContent)
                    #         .where(tableContent.c.id == int(h1_id_value))
                    #         .values(metadesc=newMeta)
                    #     )

                    #     logging.info(
                    #         f"Row {index}: Content Meta Table:{h1_id_value} --- New Meta Description = {newMeta}"
                    #     )
                    #     # Execute the update statement
                    #     with engine.begin() as connection:
                    #         connection.execute(update_stmt_meta)
                    #         infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "Meta_Updated_DB",
                    #         ] = True

                    # 3 _edocman_categories --> replace of title
                    # elif "edocman" in h1_com_value:
                    #     tableCategories = meta.tables[f"{db_prefix}edocman_categories"]
                    #     max_spacing = 90  # Maximum spacing between columns
                    #     spacing = max_spacing - len(old_h1)
                    #     logging.info(
                    #         f'Row {index}: edocman_categories: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                    #     )
                    #     # Construct the SQL update statement
                    #     update_stmt_h1 = (
                    #         update(tableCategories)
                    #         .where(tableCategories.c.id == int(h1_id_value))
                    #         .values(title=new_H1)
                    #     )
                    #     with engine.begin() as connection:
                    #         connection.execute(update_stmt_h1)
                    #         infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "H1_Updated_DB",
                    #         ] = True
                    #     # print("Value updated in the database.")

                    #     newMeta = New_Meta
                    #     update_stmt_meta = (
                    #         update(tableCategories)
                    #         .where(tableCategories.c.id == int(h1_id_value))
                    #         .values(metadesc=newMeta)
                    #     )

                    #     logging.info(
                    #         f"Row {index}: edocman_categories Meta Table:{h1_id_value} --- New Meta Description = {newMeta}"
                    #     )
                    #     # Execute the update statement
                    #     with engine.begin() as connection:
                    #         connection.execute(update_stmt_meta)
                    #         infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "Meta_Updated_DB",
                    #         ] = True

                    # elif "categories" in h1_com_value:
                    #     max_spacing = 90
                    #     tableCategories = meta.tables[f"{db_prefix}categories"]
                    #     description = row["description"]
                    #     if description:
                    #         soup = BeautifulSoup(description, "html.parser")
                    #         h1_tags = soup.find_all("h1")
                    #         for tag in h1_tags:
                    #             tag.string = new_H1
                    #         updated_string = soup.prettify()
                    #     # updated_string = description.replace(old_h1.strip(), new_H1)
                    #     if updated_string != description:
                    #         # Construct the SQL update statement
                    #         update_stmt = (
                    #             update(tableCategories)
                    #             .where(tableCategories.c.id == int(h1_id_value))
                    #             .values(description=updated_string)
                    #         )
                    #         with engine.begin() as connection:
                    #             connection.execute(update_stmt)
                    #             infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "H1_Updated_DB",
                    #         ] = True
                    #             logging.info(
                    #                 f'Row {index}: categories Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                    #             )



                        # print("Value updated in the database.")

                    # 4 __casestudies_iq --> replace of title
                    # elif "casestudies_iq" in h1_com_value:
                    #     tableCaseStudies = meta.tables[f"{db_prefix}casestudies_iq"]
                    #     max_spacing = 90  # Maximum spacing between columns
                    #     spacing = max_spacing - len(old_h1)
                    #     logging.info(
                    #         f'Row {index}: casestudies_iq Table: {old_h1}{" " * spacing}{new_H1}'
                    #     )
                    #     # Construct the SQL update statement
                    #     update_stmt = (
                    #         update(tableCaseStudies)
                    #         .where(tableCaseStudies.c.id == int(h1_id_value))
                    #         .values(title=new_H1)
                    #     )
                    #     with engine.begin() as connection:
                    #         connection.execute(update_stmt)
                    #         infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "H1_Updated_DB",
                    #         ] = True
                    #     # print("Value updated in the database.")

                    # # 4 __casestudies_iq --> replace of title
                    # elif "modules" in h1_com_value:
                    #     tableModules = meta.tables[f"{db_prefix}modules"]
                    #     params = row["params"]
                    #     if old_h1 != new_H1:
                    #         paramsJson = json.loads(params)
                    #         paramsJson["slides"]["slides0"]["title"] = new_H1
                    #         updated_string_params = json.dumps(paramsJson)
                    #         # updated_string_params = paramsJson.replace(
                    #         #     old_h1.strip(), new_H1
                    #         # )
                    #         logging.info(
                    #             f'Row {index}: Module Table: {h1_id_value}-{old_h1}{" " * spacing}{new_H1}'
                    #         )
                    #         # Construct the SQL update statement
                    #         update_stmt = (
                    #             update(tableModules)
                    #             .where(tableModules.c.id == int(h1_id_value))
                    #             .values(params=updated_string_params)
                    #         )

                    #         with engine.begin() as connection:
                    #             connection.execute(update_stmt)
                    #             infodf.loc[
                    #             (infodf["map_id"] == map_id)
                    #             & (infodf["map_data"] == map_data),
                    #             "H1_Updated_DB",
                    #             ] = True
                    #     else:
                    #         if not no_diff_records.get(h1_com_value):
                    #             no_diff_records[h1_com_value] = [
                    #                 f"{index} : {h1_id_value}"
                    #             ]
                    #         else:
                    #             no_diff_records[h1_com_value].append(
                    #                 f"{index} : {h1_id_value}"
                    #             )
                    #         logging.info(
                    #             f"Row {index} h1_id: {h1_id_value}, h1_com_value : {h1_com_value} : old and new h1 tags are same."
                    #         )
        updated_string = ''
        if title_com_value and title_id_value.isdigit():
            tableMetaComValue = meta.tables[title_com_value]
            # query = select(tableComValue).where(tableComValue.c.id == h1_id_value)
            meta_query = select(tableMetaComValue).where(
                tableMetaComValue.c.id == title_id_value
            )  # change back to above condition

            # print(query)
            try:
                # Execute the query to check if the table exists
                meta_df = pd.read_sql_query(meta_query, conn)
            except pd.io.sql.DatabaseError as e:
                logging.info(f"Row {index}: Error executing query {meta_query}: {e}")
                # print(f"Error executing query for table '{com_value}': {e}")
                continue

            # Check if the table exists
            if meta_df.empty:
                logging.info(f"Row {index}: empty dataframe")
                # print(f"Table '{com_value}' doesn't exist. Skipping...")
                continue

            for i, row in meta_df.iterrows():

                if f"{db_prefix}menu" in title_com_value:
                    tablemenu = meta.tables[f"{db_prefix}menu"]
                    params = row["params"]

                    # oldMeta = json.loads(params)
                    params = json.loads(params)
                    params["page_title"] = new_title
                    updated_string = json.dumps(params)
                    if updated_string != params:
                        update_stmt = (
                            update(tablemenu)
                            .where(tablemenu.c.id == int(title_id_value))
                            .values(params=updated_string)
                        )
                    with engine.begin() as connection:
                        connection.execute(update_stmt)
                        infodf.loc[
                            (infodf["map_id"] == map_id)
                            & (infodf["map_data"] == map_data),
                            "Title_Updated_DB",
                        ] = True
                        logging.info(
                            f'Row {index}: {db_prefix}menu title: {title_id_value}-{old_title}{" " * 20}{new_title}'
                        )

                # elif "_content" in title_com_value:
                #     tableContent = meta.tables[f"{db_prefix}content"]
                #     infodf["field_id"] = 2
                #     table = meta.tables[f"{db_prefix}fields_values"]
                #     content_query = select(table).where(
                #     table.c.item_id == title_id_value, table.c.field_id==2
                #     )
                #     content_df = pd.read_sql_query(content_query, conn)
                #     if not content_df.empty:
                #         delete_query = delete(table).where(table.c.item_id==title_id_value, table.c.field_id==2)
                #         with engine.begin() as connection:
                #             logging.info(
                #         f"Row {index}: Content Title CustomField Table: deleting existing entries with record id {title_id_value}"
                #     )
                #             connection.execute(delete_query)
                #     insert_query = insert(table).values(
                #         field_id=2, item_id=title_id_value, value=new_title
                #     )
                #     logging.info(
                #         f"Row {index}: Content Title CustomField Table:{meta_id_value} --- New Title = {new_title}"
                #     )
                #     with engine.begin() as connection:
                #         connection.execute(insert_query)
                #         infodf.loc[
                #             (infodf["map_id"] == map_id)
                #             & (infodf["map_data"] == map_data),
                #             "Title_Updated_DB",
                #         ] = True
                # elif "_edocman_categories" in title_com_value:
                #     tablemenu = meta.tables[f"{db_prefix}edocman_categories"]
                #     old_title = row["title"]
                #     if old_title != new_title:
                #         update_stmt = (
                #             update(tablemenu)
                #             .where(tablemenu.c.id == int(title_id_value))
                #             .values(title=new_title)
                #         )
                #         with engine.begin() as connection:
                #             connection.execute(update_stmt)
                #             logging.info(
                #                 f'Row {index}: {db_prefix}menu title: {title_id_value}-{old_title}{" " * 20}{new_title}'
                #             )
                #             infodf.loc[
                #                 (infodf["map_id"] == map_id)
                #                 & (infodf["map_data"] == map_data),
                #                 "Title_Updated_DB",
                #             ] = True

        # Update Meta Drescription
        if meta_com_value and meta_id_value.isdigit():
            tableMetaComValue = meta.tables[meta_com_value]
            # query = select(tableComValue).where(tableComValue.c.id == h1_id_value)
            meta_query = select(tableMetaComValue).where(
                tableMetaComValue.c.id == meta_id_value
            )  # change back to above condition

            # print(query)
            try:
                # Execute the query to check if the table exists
                meta_df = pd.read_sql_query(meta_query, conn)
            except pd.io.sql.DatabaseError as e:
                logging.info(f"Row {index}: Error executing query {meta_query}: {e}")
                # print(f"Error executing query for table '{com_value}': {e}")
                continue

            # Check if the table exists
            if meta_df.empty:
                logging.info(f"Row {index}: empty dataframe")
                # print(f"Table '{com_value}' doesn't exist. Skipping...")
                continue

            for i, row in meta_df.iterrows():

                if f"{db_prefix}menu" in meta_com_value:
                    tablemenu = meta.tables[f"{db_prefix}menu"]
                    params = row["params"]
                    oldMeta = json.loads(params)
                    if title_id_value==meta_id_value and title_com_value==meta_com_value:
                        oldMeta=json.loads(updated_string)
                    newMeta = New_Meta

                    if newMeta == "":
                        print("Invalid text or does not end with a period.")
                    elif newMeta[-1] == '"':
                        newMeta = newMeta[:-1]
                        oldMeta["menu-meta_description"] = newMeta

                    # if newMeta[-1] == '"':
                    #     newMeta = newMeta[:-1]
                    # oldMeta["menu-meta_description"] = newMeta

                    # updated_meta = params.replace(old_Meta, New_Meta)
                    update_stmt = (
                        update(tablemenu)
                        .where(tablemenu.c.id == int(meta_id_value))
                        .values(params=json.dumps(oldMeta))
                    )
                    infodf.loc[
                        (infodf["map_id"] == map_id) & (infodf["map_data"] == map_data),
                        "Meta_Updated_DB",
                    ] = True
                    with engine.begin() as connection:
                        connection.execute(update_stmt)

                # elif (
                #     "_content" in meta_com_value and 345 <= map_id <= 366
                # ):  # Done for both H1 and Title
                #     tableContent = meta.tables[f"{db_prefix}content"]

                #     newMeta = New_Meta
                #     update_stmt_meta = (
                #         update(tableContent)
                #         .where(tableContent.c.id == int(meta_id_value))
                #         .values(metadesc=newMeta)
                #     )

                #     logging.info(
                #         f"Row {index}: Content Meta Table:{meta_id_value} --- New Meta Description = {newMeta}"
                #     )
                #     # Execute the update statement
                #     with engine.begin() as connection:
                #         connection.execute(update_stmt_meta)
                #         infodf.loc[
                #             (infodf["map_id"] == map_id)
                #             & (infodf["map_data"] == map_data),
                #             "Meta_Updated_DB",
                #         ] = True

                    # if oldMeta != params:
                    #     update_stmt = update(tablemenu).where(tablemenu.c.id == int(meta_id_value)).values(params=updated_meta )
                    #     with engine.begin() as connection:
                    #         connection.execute(update_stmt)
                    # else:
                    #     print('{db_prefix}menu not updated some error')

                    # metaMetaTable = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'params'].values[0]
                    # if not pd.isna(metaMetaTable):
                    #     oldMeta = json.loads(metaMetaTable)
                    #     newMeta = extracted_text['New Meta Description']
                    #     oldMeta['menu-meta_description'] = newMeta
                    #     tableMeta = meta.tables['{db_prefix}menu']

                    #     id_to_update = menu_table.loc[(menu_table['alias'] == extracted_text['Extracted Text']) & (menu_table['path'] == extracted_text['Inspiration / Current URL (if existing page)'].split('.com/')[-1]), 'id'].values[0]
                    # update_stmt = update(tableMeta).where(tableMeta.c.id == id_to_update).values(params=json.dumps(oldMeta))

                    # logging.info(f'Menu Table: {id_to_update} \n  New Meta Description: {newMeta}')
                    # # Execute the update statement
                    # with engine.begin() as connection:
                #     connection.execute(update_stmt)

        else:
            if not empty_records.get(h1_com_value):
                empty_records[h1_com_value] = [f"{index} : {h1_id_value}"]
            else:
                empty_records[h1_com_value].append(f"{index} : {h1_id_value}")

            logging.info(
                f"Row {index}: record_url: {record_url} neither H1 not meta description or titles updated for this record."
            )
        # if meta_com_value and meta_id_value.isdigit():

    except pd.io.sql.DatabaseError as e:
        print(f"Error executing query for table '{h1_com_value}': {e}")

# update_fields_values_table(article_ids)
print("Text and Titles are upadted in DB")

# commit the changes into DB
connection.commit()
# Close the cursor and connection
conn.close()
end_time = time.time()

print("time: ", end_time - start_time)
# logging.info(f" Total H1 records: {total_records_by_type}")
# logging.info(f"Empty H1 records: {empty_records}")
# logging.info(f"No Diff {no_diff_records}")
# print("total H1 records by type")
# for key, value in total_records_by_type.items():
#     print(f"{key}: {len(value)}")
# print("\n Empty H1 records")
# for key, value in empty_records.items():
#     print(f"Empty : {len(value)}")
# print("\n No diff H1 records")
# for key, value in no_diff_records.items():
#     print(f"{key}: {len(value)}")
# infodf.to_csv(f"logs/{OUTPUT_FILE}")
