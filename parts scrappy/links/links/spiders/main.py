# import pandas as pd
# import os
# import scrapy
# import mysql.connector
# from scrapy.spiders import CrawlSpider
# from datetime import datetime
# import csv
# from scrapy.utils.project import get_project_settings
# from openpyxl import load_workbook

# class AutoCrawler(CrawlSpider):
#     name = 'autocrawler'
#     allowed_domains = ['yanmarshop.com']
#     handle_httpstatus_all = True   
#     custom_settings = {
#         'RETRY_TIMES': 2,
#         'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
#         'DOWNLOAD_FAIL_ON_DATALOSS': False,
#         'HTTPERROR_ALLOW_ALL': True
#     }

#     db_config = {
#         'host': 'localhost',
#         'user': 'root',
#         'password': 'admin@123',
#         'database': 'dilsdb'
#     }

#     original_excel_path = r'C:\Users\DEADLY\Desktop\parts scrappy\links\links\spiders\Yanmarshop_Scrape_Data (1) (1).xlsx'
#     duplicate_excel_path = r'C:\Users\DEADLY\Desktop\parts scrappy\links\links\spiders\Duplicate_Yanmarshop.xlsx'
#     direct_links_csv_path = r'C:\Users\DEADLY\Desktop\parts scrappy\direct_links.csv'
    
#     proxy = 'http://sales_sgl-bearing_com-dc:sglrayobyte123@la.residential.rayobyte.com:8000'
#     alldata = []
#     direct_links = []

#     def __init__(self, *args, **kwargs):
#         super(AutoCrawler, self).__init__(*args, **kwargs)
#         self.initialize_files()

#     def initialize_files(self):
#         if not os.path.exists(self.duplicate_excel_path):
#             df_original = pd.read_excel(self.original_excel_path)
#             df_original.to_excel(self.duplicate_excel_path, index=False)

#     def start_requests(self):
#         extracted_links = set()
#         if os.path.exists(self.direct_links_csv_path):
#             df_extracted = pd.read_csv(self.direct_links_csv_path)
#             extracted_links = set(df_extracted['Direct Links'].dropna().tolist())

#         df = pd.read_excel(self.duplicate_excel_path)
#         df_remaining = df[~df['Direct Link'].isin(extracted_links)]
#         df_remaining.to_excel(self.duplicate_excel_path, index=False)

#         for link in df_remaining.to_dict(orient='records'):
#             direct_link = link['Direct Link']
#             self.direct_links.append(direct_link)
#             yield scrapy.Request(
#                 direct_link,
#                 callback=self.parse_text,
#                 errback=self.errback_handler,
#                 meta={'proxy': self.proxy, 'handle_httpstatus_all': True},
#                 cb_kwargs={'code': link['U_SglUniqueModelCode'], 'link1': direct_link}
#             )

#     def parse_text(self, response, code, link1):
#         status = response.status
#         print(f"Processed link: {link1} | Status Code: {status}")
        
#         if status != 200:
#             self.save_failed_link(link1, status)
        
#         if status == 200:
#             inner_links = response.xpath("//li[contains(@class, 'epi-catalogue-item')]")
#             for item in inner_links:
#                 lnk = item.xpath(".//div/div/div/a/@href").get()
#                 label = item.xpath(".//div/div/div/a/div[2]/div/div[2]/h2/text()").getall()
#                 cleaned_label = ' '.join(self.clean_text(label))
#                 if lnk:
#                     full_link = response.urljoin(lnk)
#                     yield scrapy.Request(
#                         full_link,
#                         callback=self.parse_inner,
#                         meta={'proxy': self.proxy},
#                         cb_kwargs={
#                             'code': code,
#                             'link1': link1,
#                             'label': cleaned_label,
#                             'full_link': full_link
#                         }
#                     )

#     def parse_inner(self, response, code, link1, label, full_link):
#         current_date = datetime.now().strftime('%Y-%m-%d')
        
#         refs = response.css('div.m-bom-listrow span.m-sbom-ref.text-center::text').getall()
#         descriptions = response.css('div.m-bom-listrow span.m-bom-listtext a::text').getall()
#         part_numbers = response.css('div.m-bom-listrow span.m-bom-listtext::text').getall()
        
#         refs = self.clean_text(refs)
#         descriptions = self.clean_text(descriptions)
#         part_numbers = self.clean_text([pn.strip() for pn in part_numbers 
#                                         if pn.strip() and not pn.strip().isdigit() 
#                                         and "Unavailable" not in pn])

#         image_url = response.xpath("//div[@id='epi-sb-schematic']/@data-uri").get(default="No Image Found")
#         image_filename = "No Image"
#         if image_url != "No Image Found":
#             image_filename = f"{code}_{label}.svg".replace('/', '_').replace('\\', '_')
#             yield scrapy.Request(
#                 url=response.urljoin(image_url),
#                 callback=self.download_svg,
#                 meta={'proxy': self.proxy},
#                 cb_kwargs={'image_filename': image_filename}
#             )

#         for ref, description, part_number in zip(refs, descriptions, part_numbers):
#             self.alldata.append({
#                 'Code': '',
#                 'DocEntry': '',
#                 'U_Id': '',
#                 'U_SglUniqueModelCode': code,
#                 'U_Section': label,
#                 'U_PartNumber': part_number,
#                 'U_ItemNumber': ref,
#                 'U_Description': description,
#                 'U_SectonDiagram': image_filename,
#                 'U_ScraperName': "Yanmar",
#                 'U_SGLDescription': '',
#                 'U_SGLSection': '',
#                 'U_DateAdded': current_date,
#                 'U_SGLCommodity': '',
#             })

#     def save_failed_link(self, link, status_code):
#         try:
#             conn = mysql.connector.connect(**self.db_config)
#             cursor = conn.cursor()
#             sql = """INSERT INTO logs (links, response) 
#                      VALUES (%s, %s)
#                      ON DUPLICATE KEY UPDATE response=%s"""
#             cursor.execute(sql, (link, status_code, status_code))
#             conn.commit()
#             print(f"Database update: {link} | Status: {status_code}")
#         except mysql.connector.Error as err:
#             print(f"Database Error: {err}")
#         finally:
#             if 'conn' in locals() and conn.is_connected():
#                 cursor.close()
#                 conn.close()

#     def errback_handler(self, failure):
#         request = failure.request
#         if failure.check(scrapy.exceptions.HttpError):
#             response = failure.value.response
#             status = response.status
#         else:
#             status = 0  
            
#         self.save_failed_link(request.url, status)
#         print(f"Request failed: {request.url} | Error: {str(failure)}")

#     def download_svg(self, response, image_filename):
#         try:
#             images_dir = r'C:\Users\DEADLY\Desktop\parts scrappy\images'
#             os.makedirs(images_dir, exist_ok=True)
#             image_path = os.path.join(images_dir, image_filename)
#             with open(image_path, 'wb') as f:
#                 f.write(response.body)
#         except Exception as e:
#             print(f"Error saving image {image_filename}: {str(e)}")

#     def closed(self, reason):
#         try:
#             df_new = pd.DataFrame(self.alldata)
            
#             if not df_new.empty:
#                 max_rows_per_sheet = 1048576  # Excel's row limit (including header)
#                 sheet_base_name = 'Scraped_Data'
#                 sheet_names = []
#                 current_rows = 0

#                 # Check existing sheets and find the last sheet's row count
#                 if os.path.exists(self.original_excel_path):
#                     wb = load_workbook(self.original_excel_path)
#                     sheet_names = [sheet for sheet in wb.sheetnames if sheet.startswith(sheet_base_name)]
#                     sheet_names.sort(key=lambda x: int(x.split('_')[-1]) if x.split('_')[-1].isdigit() else 0)
#                     if sheet_names:
#                         last_sheet = wb[sheet_names[-1]]
#                         current_rows = last_sheet.max_row - 1  # Subtract header row
#                     wb.close()

#                 # Calculate remaining rows (accounting for header)
#                 remaining = (max_rows_per_sheet - 1) - current_rows if sheet_names else (max_rows_per_sheet - 1)

#                 # Split data into chunks that fit into sheets
#                 chunks = []
#                 if remaining > 0:
#                     chunks.append(df_new.iloc[:remaining])
#                     remaining_data = df_new.iloc[remaining:]
#                 else:
#                     remaining_data = df_new

#                 # Split remaining data into new chunks of max_rows_per_sheet - 1 (header row)
#                 while len(remaining_data) > 0:
#                     chunks.append(remaining_data.iloc[:max_rows_per_sheet - 1])
#                     remaining_data = remaining_data.iloc[max_rows_per_sheet - 1:]

#                 # Save chunks to Excel
#                 with pd.ExcelWriter(
#                     self.original_excel_path,
#                     engine='openpyxl',
#                     mode='a' if os.path.exists(self.original_excel_path) else 'w',
#                     if_sheet_exists='overlay'
#                 ) as writer:
#                     for i, chunk in enumerate(chunks):
#                         if i == 0 and sheet_names and remaining > 0:
#                             # Append to the last existing sheet
#                             sheet_name = sheet_names[-1]
#                             startrow = current_rows + 1  # Start after the last data row
#                             header = False
#                         else:
#                             # Create a new sheet
#                             existing_sheet_numbers = [
#                                 int(name.split('_')[-1]) 
#                                 for name in sheet_names 
#                                 if name.split('_')[-1].isdigit()
#                             ]
#                             sheet_num = max(existing_sheet_numbers) + 1 + i if existing_sheet_numbers else 1 + i
#                             sheet_name = f"{sheet_base_name}_{sheet_num}"
#                             startrow = 1  # Start at row 1 (header)
#                             header = True

#                         chunk.to_excel(
#                             writer,
#                             sheet_name=sheet_name,
#                             startrow=startrow,
#                             index=False,
#                             header=header
#                         )

#                 print(f"Data saved to {self.original_excel_path}")
#         except Exception as e:
#             print(f"Error saving to Excel: {e}")
#             import traceback
#             traceback.print_exc()

#         try:
#             if os.path.exists(self.direct_links_csv_path):
#                 df_direct_links = pd.read_csv(self.direct_links_csv_path)
#             else:
#                 df_direct_links = pd.DataFrame(columns=['Direct Links'])
                
#             new_links_df = pd.DataFrame({'Direct Links': self.direct_links})
#             df_combined_links = pd.concat([df_direct_links, new_links_df]).drop_duplicates()
#             df_combined_links.to_csv(self.direct_links_csv_path, index=False)
#         except Exception as e:
#             print(f"Error saving direct links: {e}")

#         print(f"Scraping complete. Total items scraped: {len(self.alldata)}")

#     def clean_text(self, text_list):
#         return [text.strip().replace('\r', '').replace('\n', '') for text in text_list if text.strip()]


# correct code 
# import pandas as pd
# import os
# import scrapy
# import mysql.connector
# from scrapy.spiders import CrawlSpider
# from datetime import datetime
# import csv
# from scrapy.utils.project import get_project_settings
# class AutoCrawler(CrawlSpider):
#     name = 'autocrawler'
#     allowed_domains = ['yanmarshop.com']
#     handle_httpstatus_all = True   
#     custom_settings = {
#         'RETRY_TIMES': 2,
#         'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
#         'DOWNLOAD_FAIL_ON_DATALOSS': False,
#         'HTTPERROR_ALLOW_ALL': True
#     }
#     db_config = {
#         'host': 'localhost',
#         'user': 'root',
#         'password': 'admin@123',
#         'database': 'dilsdb'
#     }
#     original_excel_path = r'C:\Users\DEADLY\Desktop\parts scrappy\links\links\spiders\Yanmarshop_Scrape_Data (1) (1).xlsx'
#     duplicate_excel_path = r'C:\Users\DEADLY\Desktop\parts scrappy\links\links\spiders\Duplicate_Yanmarshop.xlsx'
#     direct_links_csv_path = r'C:\Users\DEADLY\Desktop\parts scrappy\direct_links.csv'
#     proxy = 'http://sales_sgl-bearing_com-dc:sglrayobyte123@la.residential.rayobyte.com:8000'
#     alldata = []
#     direct_links = []
#     def __init__(self, *args, **kwargs):
#         super(AutoCrawler, self).__init__(*args, **kwargs)
#         self.initialize_files()
#     def initialize_files(self):
#         if not os.path.exists(self.duplicate_excel_path):
#             df_original = pd.read_excel(self.original_excel_path)
#             df_original.to_excel(self.duplicate_excel_path, index=False)
#     def start_requests(self):
#         extracted_links = set()
#         if os.path.exists(self.direct_links_csv_path):
#             df_extracted = pd.read_csv(self.direct_links_csv_path)
#             extracted_links = set(df_extracted['Direct Links'].dropna().tolist())
#         df = pd.read_excel(self.duplicate_excel_path)
#         df_remaining = df[~df['Direct Link'].isin(extracted_links)]
#         df_remaining.to_excel(self.duplicate_excel_path, index=False)
#         for link in df_remaining.to_dict(orient='records'):
#             direct_link = link['Direct Link']
#             self.direct_links.append(direct_link)
#             yield scrapy.Request(
#                 direct_link,
#                 callback=self.parse_text,
#                 errback=self.errback_handler,
#                 meta={'proxy': self.proxy},
#                 cb_kwargs={'code': link['U_SglUniqueModelCode'], 'link1': direct_link}
#             )
#     def parse_text(self, response, code, link1):
#         status = response.status
#         print(f"Processed link: {link1} | Status Code: {status}")
      
#         if status != 200:
#             self.save_failed_link(link1, status)
      
#         if status == 200:
#             inner_links = response.xpath("//li[contains(@class, 'epi-catalogue-item')]")
#             for item in inner_links:
#                 lnk = item.xpath(".//div/div/div/a/@href").get()
#                 label = item.xpath(".//div/div/div/a/div[2]/div/div[2]/h2/text()").getall()
#                 cleaned_label = ' '.join(self.clean_text(label))
#                 if lnk:
#                     full_link = response.urljoin(lnk)
#                     yield scrapy.Request(
#                         full_link,
#                         callback=self.parse_inner,
#                         meta={'proxy': self.proxy},
#                         cb_kwargs={
#                             'code': code,
#                             'link1': link1,
#                             'label': cleaned_label,
#                             'full_link': full_link
#                         }
#                     )
#     def parse_inner(self, response, code, link1, label, full_link):
#         current_date = datetime.now().strftime('%Y-%m-%d')
      
#         refs = response.css('div.m-bom-listrow span.m-sbom-ref.text-center::text').getall()
#         descriptions = response.css('div.m-bom-listrow span.m-bom-listtext a::text').getall()
#         part_numbers = response.css('div.m-bom-listrow span.m-bom-listtext::text').getall()
      
#         refs = self.clean_text(refs)
#         descriptions = self.clean_text(descriptions)
#         part_numbers = self.clean_text([pn.strip() for pn in part_numbers 
#                                         if pn.strip() and not pn.strip().isdigit() 
#                                         and "Unavailable" not in pn])
#         image_url = response.xpath("//div[@id='epi-sb-schematic']/@data-uri").get(default="No Image Found")
#         image_filename = "No Image"
#         if image_url != "No Image Found":
#             image_filename = f"{code}_{label}.svg".replace('/', '_').replace('\\', '_')
#             yield scrapy.Request(
#                 url=response.urljoin(image_url),
#                 meta={'proxy': self.proxy},
#                 callback=self.download_svg,
              
#                 cb_kwargs={'image_filename': image_filename}
#             )
#         for ref, description, part_number in zip(refs, descriptions, part_numbers):
#             self.alldata.append({
#                 'Code': '',
#                 'DocEntry': '',
#                 'U_Id': '',
#                 'U_SglUniqueModelCode': code,
#                 'U_Section': label,
#                 'U_PartNumber': part_number,
#                 'U_ItemNumber': ref,
#                 'U_Description': description,
#                 'U_SectonDiagram': image_filename,
#                 'U_ScraperName': "Yanmar",
#                 'U_SGLDescription': '',
#                 'U_SGLSection': '',
#                 'U_DateAdded': current_date,
#                 'U_SGLCommodity': '',
#             })
#     def save_failed_link(self, link, status_code):
#         try:
#             conn = mysql.connector.connect(**self.db_config)
#             cursor = conn.cursor()
#             sql = """INSERT INTO logs (links, response) 
#                      VALUES (%s, %s)
#                      ON DUPLICATE KEY UPDATE response=%s"""
#             cursor.execute(sql, (link, status_code, status_code))
#             conn.commit()
#             print(f"Database update: {link} | Status: {status_code}")
#         except mysql.connector.Error as err:
#             print(f"Database Error: {err}")
#         finally:
#             if 'conn' in locals() and conn.is_connected():
#                 cursor.close()
#                 conn.close()
#     def errback_handler(self, failure):
#         request = failure.request
#         if failure.check(scrapy.exceptions.HttpError):
#             response = failure.value.response
#             status = response.status
#         else:
#             status = 0  
          
#         self.save_failed_link(request.url, status)
#         print(f"Request failed: {request.url} | Error: {str(failure)}")
#     def download_svg(self, response, image_filename):
#         try:
#             images_dir = r'C:\Users\DEADLY\Desktop\parts scrappy\images'
#             os.makedirs(images_dir, exist_ok=True)
#             image_path = os.path.join(images_dir, image_filename)
#             with open(image_path, 'wb') as f:
#                 f.write(response.body)
#         except Exception as e:
#             print(f"Error saving image {image_filename}: {str(e)}")
#     def save_to_database(self):
#         try:
#             conn = mysql.connector.connect(**self.db_config)
#             cursor = conn.cursor()
#             insert_sql = """
#                 INSERT INTO scraped_data 
#                     (Code, DocEntry, U_Id, U_SglUniqueModelCode, U_Section, U_PartNumber, 
#                     U_ItemNumber, U_Description, U_SectonDiagram, U_ScraperName, 
#                     U_SGLDescription, U_SGLSection, U_DateAdded, U_SGLCommodity)
#                 VALUES 
#                     (%(Code)s, %(DocEntry)s, %(U_Id)s, %(U_SglUniqueModelCode)s, %(U_Section)s, 
#                     %(U_PartNumber)s, %(U_ItemNumber)s, %(U_Description)s, %(U_SectonDiagram)s, 
#                     %(U_ScraperName)s, %(U_SGLDescription)s, %(U_SGLSection)s, %(U_DateAdded)s, 
#                     %(U_SGLCommodity)s)
#             """
#             cursor.executemany(insert_sql, self.alldata)
#             conn.commit()
#             print(f"Successfully inserted {len(self.alldata)} records into the database.")
#         except mysql.connector.Error as e:
#             print(f"Database error during insertion: {e}")
#             conn.rollback()
#         finally:
#             if conn.is_connected():
#                 cursor.close()
#                 conn.close()
#     def closed(self, reason):
#         try:
#             if self.alldata:
#                 self.save_to_database()
          
#             try:
#                 if os.path.exists(self.direct_links_csv_path):
#                     df_direct_links = pd.read_csv(self.direct_links_csv_path)
#                 else:
#                     df_direct_links = pd.DataFrame(columns=['Direct Links'])
                  
#                 new_links_df = pd.DataFrame({'Direct Links': self.direct_links})
#                 df_combined_links = pd.concat([df_direct_links, new_links_df]).drop_duplicates()
#                 df_combined_links.to_csv(self.direct_links_csv_path, index=False)
#             except Exception as e:
#                 print(f"Error saving direct links: {e}")
#             print(f"Scraping complete. Total items scraped: {len(self.alldata)}")
#         except Exception as e:
#             print(f"Error during closing: {e}")
#             import traceback
#             traceback.print_exc()
#     def clean_text(self, text_list):
#         return [text.strip().replace('\r', '').replace('\n', '') for text in text_list if text.strip()]





import pandas as pd
import os
import random
import scrapy
import mysql.connector
from scrapy.spiders import CrawlSpider
from datetime import datetime
import csv
from scrapy.utils.project import get_project_settings


# Function to get a random proxy from the hardcoded list
def get_random_proxy():
    return "http://" + random.choice(proxy_list)

class AutoCrawler(CrawlSpider):
    name = 'autocrawler'
    allowed_domains = ['yanmarshop.com']
    handle_httpstatus_all = True   
    custom_settings = {
        'RETRY_TIMES': 2,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],
        'DOWNLOAD_FAIL_ON_DATALOSS': False,
        'HTTPERROR_ALLOW_ALL': True
    }
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'admin@123',
        'database': 'dilsdb'
    }
    original_excel_path = r'C:\Users\DEADLY\Desktop\parts scrappy\links\links\spiders\Yanmarshop_Scrape_Data (1) (1).xlsx'
    duplicate_excel_path = r'C:\Users\DEADLY\Desktop\parts scrappy\links\links\spiders\Duplicate_Yanmarshop.xlsx'
    direct_links_csv_path = r'C:\Users\DEADLY\Desktop\parts scrappy\direct_links.csv'
    alldata = []
    direct_links = []

    def __init__(self, *args, **kwargs):
        super(AutoCrawler, self).__init__(*args, **kwargs)
        self.initialize_files()

    def initialize_files(self):
        if not os.path.exists(self.duplicate_excel_path):
            df_original = pd.read_excel(self.original_excel_path)
            df_original.to_excel(self.duplicate_excel_path, index=False)

    def start_requests(self):
        extracted_links = set()
        if os.path.exists(self.direct_links_csv_path):
            df_extracted = pd.read_csv(self.direct_links_csv_path)
            extracted_links = set(df_extracted['Direct Links'].dropna().tolist())
        df = pd.read_excel(self.duplicate_excel_path)
        df_remaining = df[~df['Direct Link'].isin(extracted_links)]
        df_remaining.to_excel(self.duplicate_excel_path, index=False)
        for link in df_remaining.to_dict(orient='records'):
            direct_link = link['Direct Link']
            self.direct_links.append(direct_link)
            yield scrapy.Request(
                direct_link,
                callback=self.parse_text,
                errback=self.errback_handler,
                meta={'proxy': get_random_proxy()},
                cb_kwargs={'code': link['U_SglUniqueModelCode'], 'link1': direct_link}
            )

    def parse_text(self, response, code, link1):
        status = response.status
        print(f"Processed link: {link1} | Status Code: {status}")
        if status != 200:
            self.save_failed_link(link1, status)
        if status == 200:
            inner_links = response.xpath("//li[contains(@class, 'epi-catalogue-item')]")
            for item in inner_links:
                lnk = item.xpath(".//div/div/div/a/@href").get()
                label = item.xpath(".//div/div/div/a/div[2]/div/div[2]/h2/text()").getall()
                cleaned_label = ' '.join(self.clean_text(label))
                if lnk:
                    full_link = response.urljoin(lnk)
                    yield scrapy.Request(
                        full_link,
                        callback=self.parse_inner,
                        meta={'proxy': get_random_proxy()},
                        cb_kwargs={
                            'code': code,
                            'link1': link1,
                            'label': cleaned_label,
                            'full_link': full_link
                        }
                    )

    def parse_inner(self, response, code, link1, label, full_link):
        current_date = datetime.now().strftime('%Y-%m-%d')
        refs = response.css('div.m-bom-listrow span.m-sbom-ref.text-center::text').getall()
        descriptions = response.css('div.m-bom-listrow span.m-bom-listtext a::text').getall()
        part_numbers = response.css('div.m-bom-listrow span.m-bom-listtext::text').getall()
        refs = self.clean_text(refs)
        descriptions = self.clean_text(descriptions)
        part_numbers = self.clean_text([pn.strip() for pn in part_numbers 
                                        if pn.strip() and not pn.strip().isdigit() and "Unavailable" not in pn])
        image_url = response.xpath("//div[@id='epi-sb-schematic']/@data-uri").get(default="No Image Found")
        image_filename = "No Image"
        if image_url != "No Image Found":
            image_filename = f"{code}_{label}.svg".replace('/', '_').replace('\\', '_')
            yield scrapy.Request(
                url=response.urljoin(image_url),
                meta={'proxy': get_random_proxy()},
                callback=self.download_svg,
                cb_kwargs={'image_filename': image_filename}
            )
        for ref, description, part_number in zip(refs, descriptions, part_numbers):
            self.alldata.append({
                'Code': '',
                'DocEntry': '',
                'U_Id': '',
                'U_SglUniqueModelCode': code,
                'U_Section': label,
                'U_PartNumber': part_number,
                'U_ItemNumber': ref,
                'U_Description': description,
                'U_SectonDiagram': image_filename,
                'U_ScraperName': "Yanmar",
                'U_SGLDescription': '',
                'U_SGLSection': '',
                'U_DateAdded': current_date,
                'U_SGLCommodity': '',
            })

    def save_failed_link(self, link, status_code):
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            sql = """INSERT INTO logs (links, response) 
                     VALUES (%s, %s)
                     ON DUPLICATE KEY UPDATE response=%s"""
            cursor.execute(sql, (link, status_code, status_code))
            conn.commit()
            print(f"Database update: {link} | Status: {status_code}")
        except mysql.connector.Error as err:
            print(f"Database Error: {err}")
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()

    def errback_handler(self, failure):
        request = failure.request
        if failure.check(scrapy.exceptions.HttpError):
            response = failure.value.response
            status = response.status
        else:
            status = 0  
        self.save_failed_link(request.url, status)
        print(f"Request failed: {request.url} | Error: {str(failure)}")

    def download_svg(self, response, image_filename):
        try:
            images_dir = r'C:\Users\DEADLY\Desktop\parts scrappy\images'
            os.makedirs(images_dir, exist_ok=True)
            image_path = os.path.join(images_dir, image_filename)
            with open(image_path, 'wb') as f:
                f.write(response.body)
        except Exception as e:
            print(f"Error saving image {image_filename}: {str(e)}")

    def save_to_database(self):
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            insert_sql = """
                INSERT INTO scraped_data 
                    (Code, DocEntry, U_Id, U_SglUniqueModelCode, U_Section, U_PartNumber, 
                    U_ItemNumber, U_Description, U_SectonDiagram, U_ScraperName, 
                    U_SGLDescription, U_SGLSection, U_DateAdded, U_SGLCommodity)
                VALUES 
                    (%(Code)s, %(DocEntry)s, %(U_Id)s, %(U_SglUniqueModelCode)s, %(U_Section)s, 
                    %(U_PartNumber)s, %(U_ItemNumber)s, %(U_Description)s, %(U_SectonDiagram)s, 
                    %(U_ScraperName)s, %(U_SGLDescription)s, %(U_SGLSection)s, %(U_DateAdded)s, 
                    %(U_SGLCommodity)s)
            """
            cursor.executemany(insert_sql, self.alldata)
            conn.commit()
            print(f"Successfully inserted {len(self.alldata)} records into the database.")
        except mysql.connector.Error as e:
            print(f"Database error during insertion: {e}")
            conn.rollback()
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def closed(self, reason):
        try:
            if self.alldata:
                self.save_to_database()
            try:
                if os.path.exists(self.direct_links_csv_path):
                    df_direct_links = pd.read_csv(self.direct_links_csv_path)
                else:
                    df_direct_links = pd.DataFrame(columns=['Direct Links'])
                new_links_df = pd.DataFrame({'Direct Links': self.direct_links})
                df_combined_links = pd.concat([df_direct_links, new_links_df]).drop_duplicates()
                df_combined_links.to_csv(self.direct_links_csv_path, index=False)
            except Exception as e:
                print(f"Error saving direct links: {e}")
            print(f"Scraping complete. Total items scraped: {len(self.alldata)}")
        except Exception as e:
            print(f"Error during closing: {e}")
            import traceback
            traceback.print_exc()

    def clean_text(self, text_list):
        return [text.strip().replace('\r', '').replace('\n', '') for text in text_list if text.strip()]


