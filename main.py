import pandas as pd
import requests
from bs4 import BeautifulSoup
from pprint import pprint
import math
import lxml
from datetime import date
import logging
from logging.handlers import RotatingFileHandler
import time

# Set up the rotating file handler
log_file = 'status.log'
handler = RotatingFileHandler(log_file, maxBytes=1024*1024, backupCount=1)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Configure the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(handler)

# Read in current master csv
master = pd.read_csv('data/scrape_master.csv')
master_id_lst = list(set(master['job_id'].tolist()))
master_id = [int(job_id) for job_id in master_id_lst]

def get_links(num_iterations, max_num_jobs):
    link_master = []
    for offset in range(num_iterations):
        scrape_url = f"https://www.sportspeople.com.au/jobs?Count=200&Offset={offset}"
        response = requests.get(scrape_url)
        soup = BeautifulSoup(response.content, "html.parser")
        
        links = ["https://www.sportspeople.com.au/" + a['href'] for a in soup.find_all('a', class_='position_link')]
        
        for link in links:
            link_master.append(link)
            
    # if max_num_jobs == len(link_master):
    #     pprint("All job links captured, moving on to individual job scrape")
    #     pprint(len(link_master))
    
    return link_master

init_url = "https://www.sportspeople.com.au/jobs?Count=200"
init_response = requests.get(init_url)
init_soup = BeautifulSoup(init_response.content, "html.parser")
try:
    max_num_jobs = int(init_soup.find('figure', class_='pager-block').find('figcaption').get_text(strip=True).split('of ')[1].split(' Jobs')[0])
    
except:
    max_num_jobs = int(init_soup.find('figure', class_='pager-block').find('figcaption').get_text(strip=True).split(' Jobs')[0].split('Showing ')[1])

num_iterations = math.ceil(max_num_jobs / 200)

link_master = get_links(num_iterations, max_num_jobs)

job_item_lst = []
for job_url in link_master:
    job_id = int(job_url.split('https://www.sportspeople.com.au//jobs/')[1].split('-')[0])
    
    if job_id not in master_id:
        # pprint(job_url)
            
        job_response = requests.get(job_url)
        job_soup = BeautifulSoup(job_response.content, 'lxml')
        
        job_title = job_soup.find('h2', class_='job-teaser__title').get_text(strip=True)
        organisation_name = job_soup.find('h3', class_='job-teaser__employer-name').get_text(strip=True)
        emp_type = job_soup.find('span', itemprop='employmentType').get_text(strip=True)
        
        try:
            location_city = job_soup.find('span', class_='icon-location__city').get_text(strip=True)
            location_state = job_soup.find('span', class_='icon-location__state').get_text(strip=True)
            location_country = job_soup.find('span', class_='icon-location__country').get_text(strip=True)
        except:
            location_city = None
            location_state = None
            location_country = None
            
        try:
            closing_date = job_soup.find('span', class_='job-teaser__closing__date online-list').get_text(strip=True)
        except:
            closing_date = None
        
        try:
            salary_ele = job_soup.find('li', itemprop='baseSalary')
            
            try:
                salary_desc = salary_ele.find('span', class_='icon-location__city').get_text(strip=True)
            except:
                salary_desc = None
                
            try:
                min_salary = salary_ele.find('meta', itemprop='minValue')['content']
            except:
                min_salary = None
                
            try:
                max_salary = salary_ele.find('meta', itemprop='maxValue')['content']
            except:
                max_salary = None
            
        except:
            salary= None
            min_salary = None
            max_salary = None
        
        try:
            qualifications_ul = job_soup.find_all('ul', class_='custom-bullets')
            
            try:
                essential_criteria_sep = [li.get_text(strip=True) for li in qualifications_ul[0].find_all('li')] if qualifications_ul[0] else []
                essential_criteria = ','.join(essential_criteria_sep)
            except:
                essential_criteria = None
            
            try:
                desirable_criteria_sep = [li.get_text(strip=True) for li in qualifications_ul[1].find_all('li')] if qualifications_ul[1] else []
                desirable_criteria = ', '.join(desirable_criteria_sep)
            except:
                desirable_criteria = None

        except:
            essential_criteria = None
            desirable_criteria = None
        
        try:
            org_contact_name = job_soup.find('div', class_='job-teaser__icon icon-user').get_text(strip=True)
            
            if "," in org_contact_name:
                org_contact_role = org_contact_name.split(", ")[1].strip()
                org_contact_name = org_contact_name.split(", ")[0].strip()
            elif '(' in org_contact_name:
                org_contact_role = org_contact_name.split(" (")[1].replace(')','').strip()
                org_contact_name = org_contact_name.split(" (")[0].strip()
            else:
                org_contact_role = None
            
        except:
            org_contact_name = None
            org_contact_role = None
        
        try:
            org_contact_phone = job_soup.find('div', class_='job-teaser__icon icon-phone').get_text(strip=True).strip()
            if ', ' in org_contact_phone:
                org_contact_phone = org_contact_phone.split(', ')[0]
        except:
            org_contact_phone = None
        
        email_links = [i.lower().replace('mailto:','') for i in list(set([a['href'] for a in job_soup.find_all('a', href=True) if '@' in a['href']]))]
        
        if len(email_links) > 1:
            org_contact_email = ', '.join(email_links)
        elif len(email_links) == 1:
            org_contact_email = email_links[0]
        else:
            org_contact_email = None
            
        try:
            attachment_ele = job_soup.find('ul',class_='AttachmentList')
            attachments_sep = ["https://www.sportspeople.com.au" + i['href'] for i in attachment_ele.find_all('a', href=True)]
            
            if len(attachments_sep) > 1:
                job_attachments = ','.join(attachments_sep)
            else:
                job_attachments = attachments_sep[0]  
        except:
            job_attachments = None
            
            
        try:
            description_long = job_soup.find('div',itemprop='description').get_text(strip=True)
        except:
            description_long = None
            
        try:
            category_ele = job_soup.find('ul',class_= 'browse-classification')
            categories_sep = [i.get_text(strip=True).split('(')[0] for i in category_ele.find_all('li')]
            categories = ','.join(categories_sep)
        except:
            categories = None
            
            
        try:
            sport_ele = job_soup.find('ul',class_= 'browse-tag')
            sports_sep = [i.get_text(strip=True).split('(')[0] for i in sport_ele.find_all('li')]
            if 'Cooljobs' in sports_sep:
                is_cool_flag = True
                sports_sep.remove('Cooljobs')
            else:
                is_cool_flag = False
                
            sports = ','.join(sports_sep)
        except:
            sports = None
            is_cool_flag = False
            
        job_item_lst.append([
            job_id, job_url, job_title, organisation_name, emp_type, 
            location_city, location_state, location_country, closing_date, 
            salary_desc, min_salary, max_salary, 
            essential_criteria, desirable_criteria, 
            org_contact_name, org_contact_role, org_contact_phone, org_contact_email,
            job_attachments, description_long, sports, is_cool_flag
        ])
        
        time.sleep(2)
        
    else:
        continue

if len(job_item_lst) > 0:
    job_df = pd.DataFrame(job_item_lst, columns = [
    "job_id", "job_url", "job_title", "organisation_name", "emp_type", 
    "location_city", "location_state", "location_country", "closing_date", 
    "salary_desc", "min_salary", "max_salary", 
    "essential_criteria", "desirable_criteria", 
    "org_contact_name", "org_contact_role", "org_contact_phone", "org_contact_email",
    "job_attachments","description_long", "sports", "is_cool_flag"
    ])

    job_df['date_created'] = date.today()
    
    updated_master_df = pd.concat([master,job_df])
    updated_master_df.reset_index(drop=True, inplace=True)


    updated_master_df.to_csv('data/scrape_master.csv', index = False)
    
    # Log number of new jobs
    log_new_jobs = len(job_df)
    logging.info("Logging new jobs: %d", log_new_jobs)
    print("Logging new jobs:", log_new_jobs)
    
else:
    # Log message when there are no new jobs
    logging.info("No new jobs to log.")
    print("No new jobs to log.")
