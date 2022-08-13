#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraping SAC

Created on Tue Jun 29 11:37:59 2022

@author: shiqimeng
@version: 1.0
"""
from joblib import Parallel, delayed
import os, csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
import time
from random import uniform
import numpy as np
# pip install pyarrow
import pyarrow as pa
import pyarrow.parquet as pq

# pip install -U selenium
# pip install webdriver-manager

# Save path and output file names
save_path = '<path>'

firmind = save_path + os.sep + 'firmind.csv'  # firm individual file
ind_ch = save_path + os.sep + 'ind_ch.csv'  # change in records file

# Write header
with open(firmind, 'w', newline = '') as output:
    writer = csv.writer(output)
    writer.writerow(['姓名','性别','学历','登记编号','执业机构','执业岗位','登记日期',
                     '登记变更记录','诚信记录'])

with open(ind_ch, 'w', newline = '') as output:
    writer = csv.writer(output)
    writer.writerow(['姓名','登记编号','登记日期','执业机构','执业岗位','登记状态',
                     '离职登记日期'])
    
# Set webdriver
driver_path = Service('/Users/shiqimeng/Desktop/MFIN7035_Big_Data/Assignments/Final/chromedriver')
browser = webdriver.Chrome(service = driver_path)

# Since firm level data tables are small, directly write to parquet file
# ----------------- Scrape firmlic -------------------------------------
# First go to firmlic page
firmlic_path = 'https://exam.sac.net.cn/pages/registration/sac-publicity-report.html'
browser.get(firmlic_path)
time.sleep(uniform(1,1.3))

# Select type of firm
select_element = browser.find_element(By.XPATH,'//*[@id="otcId"]')
select_object = Select(select_element)
all_available_options = [i.text for i in select_object.options]

for option in all_available_options:
    select_object.select_by_visible_text(option)

    # get table header
    header = browser.find_elements(By.XPATH,'/html/body/div/table[3]/tbody[1]/tr')
    header_ls = header[0].text.split(' ')

    # table body
    body = browser.find_elements(By.XPATH,'//*[@id="publicityOtherList"]')
    body_f = body[0].text.split('\n') # split each firm
    if body_f[0] == '':  # if table is empty, ignore it
        continue
    body_split = list(np.transpose(np.array([i.split(' ') for i in body_f]))) # split feature within firm

    # write table
    table = pa.table(body_split, names=header_ls)
    pq.write_table(table, save_path + os.sep + 'firmlic_{}.parquet'.format(option))

browser.quit()

# ----------------- Scrape firmind & ind_ch -------------------------------------
# First go to firmlic page
driver_path = Service('/Users/shiqimeng/Desktop/MFIN7035_Big_Data/Assignments/Final/chromedriver')
browser = webdriver.Chrome(service = driver_path)
firmlic_path = 'https://exam.sac.net.cn/pages/registration/sac-publicity-report.html'
browser.get(firmlic_path)
time.sleep(uniform(1,1.3))

# for option in all_available_options:
select_element = browser.find_element(By.XPATH,'//*[@id="otcId"]')
select_object = Select(select_element)
all_available_options = [i.text for i in select_object.options]

browser.close()

for option in all_available_options: # modify option to resume scraping
    browser = webdriver.Chrome(service = driver_path)
    firmlic_path = 'https://exam.sac.net.cn/pages/registration/sac-publicity-report.html'
    browser.get(firmlic_path)
    time.sleep(uniform(1,1.3))
    
    select_element = browser.find_element(By.XPATH,'//*[@id="otcId"]')
    select_object = Select(select_element)
    select_object.select_by_visible_text(option)
    
    time.sleep(uniform(2,3))
    
    # Get no of table rows for a type of firm
    row_ls = browser.find_elements(By.XPATH, '//*[@id="publicityOtherList"]')[0].text.split('\n')
    row_no = len(row_ls)
    
    browser.close()
    
    def scrape_comp(i):
        '''
        Get the licensees for a row/firm
        inout: i - row number
        '''
        browser = webdriver.Chrome(service = driver_path)
        firmlic_path = 'https://exam.sac.net.cn/pages/registration/sac-publicity-report.html'
        browser.get(firmlic_path)
        time.sleep(uniform(2,3))
        
        select_element = browser.find_element(By.XPATH,'//*[@id="otcId"]')
        select_object = Select(select_element)
        select_object.select_by_visible_text(option)

        # record window handle
        main_win = browser.current_window_handle
        
        try:
            browser.execute_script('window.scrollTo(0,document.body.scrollHeight)') # scroll down to bottom
            f_name = browser.find_element(By.XPATH, '//*[@id="publicityOtherList"]/tr[{}]/td[2]/a'.format(str(i+1)))
        except:
            try:
                time.sleep(uniform(1,2)) # if error try again
                browser.execute_script('window.scrollTo(0,document.body.scrollHeight)') # scroll down to bottom
                f_name = browser.find_element(By.XPATH, '//*[@id="publicityOtherList"]/tr[{}]/td[2]/a'.format(str(i+1)))
            except:  # check whether i > max_row_no
                r_ls = browser.find_elements(By.XPATH, '//*[@id="publicityOtherList"]')[0].text.split('\n')
                max_row_no = len(r_ls)
                if i >= max_row_no:
                    print('ERROR: {} greater than {}'.format(i,max_row_no-1))
                    i = max_row_no-1
        
        f_name_txt = f_name.text
        f_name.click()
        
        time.sleep(uniform(1.8,2))
        
        # go to pop-up window (a specific firm)
        browser.switch_to.window(browser.window_handles[1])
        
        # get max page number
        maxpg_txt = browser.find_element(By.XPATH,'//*[@id="sp_1"]').text
        maxpg_int = int(maxpg_txt)
        
        # scrape licensee within a firm (single page)
        def scrape_licensee():
            body = browser.find_elements(By.XPATH, '//*[@id="list"]/tbody')
            body_ind = body[0].text.split('\n')  # split each individual
            body_split = [i.rsplit(' ', 8) for i in body_ind] # split feature within ind
            
            # write into csv
            for i in body_split:
                with open(firmind, 'a', newline = '') as output:
                    writer = csv.writer(output)
                    writer.writerow(i)
                    
        def scrape_indrecord(page=0):
            if page == maxpg_int:
                time.sleep(uniform(1,2))
                row_ls2 = browser.find_elements(By.XPATH, '//*[@id="list"]/tbody')[0].text.split('\n')
                row_no2 = len(row_ls2)
                
                for i in range(row_no2):
                    
                    try:
                        browser.execute_script('window.scrollTo(0,document.body.scrollHeight)') # scroll down to bottom
                        time.sleep(uniform(2,3)) 
                        name = browser.find_element(By.XPATH, '//*[@id="{}"]/td[2]/a'.format(i+1))
                    except:
                        try:
                            time.sleep(uniform(1,2)) # if error, wait and try again
                            browser.execute_script('window.scrollTo(0,document.body.scrollHeight)') # scroll down to bottom
                            time.sleep(uniform(1,2)) 
                            name = browser.find_element(By.XPATH, '//*[@id="{}"]/td[2]/a'.format(i+1))
                        except: # check whether i > max_row
                            t_len = browser.find_elements(By.XPATH, '//*[@id="list"]/tbody')[0].text.split('\n')
                            max_row = len(t_len)
                            if i >= max_row:
                                print('ERROR: {} greater than {}'.format(i,max_row-1))
                                i = max_row-1
                            
                    name_txt = name.text
                    
                    firm_win = browser.current_window_handle
                    name.click()
                    time.sleep(uniform(2,3))
                    
                    browser.switch_to.window(browser.window_handles[2])
                    
                    body_re = browser.find_elements(By.XPATH, '//*[@id="publicityList"]')
                    body_table_re = body_re[0].text.split('\n')
                    body_re_split = [i.split(' ') for i in body_table_re] 
                    
                    # write output
                    for i in body_re_split:
                        with open(ind_ch, 'a', newline = '') as output:
                            writer = csv.writer(output)
                            writer.writerow([name_txt]+i)
                    
                    # close pop-up window
                    browser.close()
                    # switch back
                    browser.switch_to.window(firm_win)

            else:
                for i in range(100):
                    try:
                        browser.execute_script('window.scrollTo(0,document.body.scrollHeight)') # scroll down to bottom
                        time.sleep(uniform(2,3)) 
                        name = browser.find_element(By.XPATH, '//*[@id="{}"]/td[2]/a'.format(i+1))
                    except:
                        try:
                            time.sleep(uniform(1,2)) # if error, wait and try again
                            browser.execute_script('window.scrollTo(0,document.body.scrollHeight)') # scroll down to bottom
                            time.sleep(uniform(1,2)) 
                            name = browser.find_element(By.XPATH, '//*[@id="{}"]/td[2]/a'.format(i+1))
                        except:
                            print(f_name_txt + '; ' + str(page) + '; ' + str(i))
                    
                    name_txt = name.text
                    
                    firm_win = browser.current_window_handle
                    name.click()
                    time.sleep(uniform(0.5,1))
                    
                    browser.switch_to.window(browser.window_handles[2])
                    
                    body_re = browser.find_elements(By.XPATH, '//*[@id="publicityList"]')
                    body_table_re = body_re[0].text.split('\n')
                    body_re_split = [i.split(' ') for i in body_table_re] 
                    
                    # write output
                    for i in body_re_split:
                        with open(ind_ch, 'a', newline = '') as output:
                            writer = csv.writer(output)
                            writer.writerow([name_txt]+i)
                    
                    # close pop-up window
                    browser.close()
                    # switch back
                    browser.switch_to.window(firm_win)
        
        # Scrape 1st page
        scrape_licensee()
        scrape_indrecord(page = 1)
        
        # next page loop
        for i in range(maxpg_int-1):
            next_pg = browser.find_element(By.XPATH, '//*[@id="next_t"]/span')
            next_pg.click()
            time.sleep(uniform(1.5,2))
            scrape_licensee()
            scrape_indrecord(page = i+2)
        
        # close pop-up window
        browser.close()
        # switch back
        browser.switch_to.window(main_win)
        browser.close()
        time.sleep(uniform(1.5,2))

    # Use joblib for multiprocessing
    Parallel(n_jobs = 4, backend='threading')(delayed(scrape_comp)(i) for i in range(row_no)) # row_no
    
    