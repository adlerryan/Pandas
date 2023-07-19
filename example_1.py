import pandas as pd
import numpy as np
import glob
import os
from datetime import date
from pathlib import Path
import time


# Functions

def create_rename_mapper(df, raw_key = 'raw fields'):
    """
    Create mapping hash table based on the data dictionary columns "raw fields"/"raw fields substitute" 
    and "mapped fields".
    @df: Dataframe from the one tab of the data dictionary excel file.
    @raw_key: The raw column key from data dictionary, "raw fields" or "raw fields substitute".
    """
    arr = df.loc[~pd.isnull(df[raw_key])][[raw_key, 'mapped fields']].values
    mapper = {}
    for e in arr:
        mapper[e[0]] = e[1]
    return mapper   

def add_claimtype(data):
    """
    Derive claimtype in medical claims.
    @data: Medical claims dataframe.
    """
    # Need full file path
    excel = 'file1'
    tob_mapping = pd.read_excel(excel, sheet_name='Advanced_ProviderCategory Map', dtype=str, engine='openpyxl').replace('nan', np.nan)

    tob = 'typeOfBill'
    data[tob] = data.groupby('claimId')[tob].transform('first')
    data.loc[data[tob] == '999', tob] = np.nan

    fill_tob = lambda x: x if pd.isnull(x) else ('0'+x+'1' if len(x) == 2 else ('0'+x if len(x) == 3 else x))
    data[tob] = data[tob].map(fill_tob)
    data[tob] = data[tob].str.zfill(4)

    tob_mapping.rename(columns={'Type of Bill': 'typeOfBill',
                                'providerCategory':'providerCategoryHeader'}, inplace=True)
    tob_mapping = tob_mapping.drop_duplicates(subset = ['typeOfBill'])
    
    data = data.merge(tob_mapping[[tob, 'providerCategoryHeader']],
                      on = tob,
                      how = 'left')
    
    data['providerCategoryHeader'] = data['providerCategoryHeader'].fillna('professional')
    data['claimType'] = data['providerCategoryHeader'].map({'inpatient': 'I', 'outpatient': 'I', 'other': 'I', 'professional': 'P'})

    return data

def create_member_months(elig):
    """
    Create member months data based on eligibility, each row of member months data will be an eligible month
    for a member.
    """
    df = elig[['eligibilityStartDate', 'eligibilityEndDate', 'memberId', 'lineOfBusiness']].drop_duplicates()

    msk = df['eligibilityStartDate'].dt.year < 2016
    df.loc[msk, 'eligibilityStartDate'] = pd.to_datetime('2016-01-01')
    
    
    msk2 = df['eligibilityEndDate'].dt.year > date.today().year
    df.loc[msk2, 'eligibilityEndDate'] = pd.to_datetime(str(date.today().year) + '-12-31')

    msk = pd.isnull(df['eligibilityStartDate'])
    msk &= pd.isnull(df['eligibilityEndDate'])
    df = df.loc[~msk]

    df.loc[pd.isnull(df['eligibilityStartDate']), 'eligibilityStartDate'] = pd.to_datetime('2017-01-01')
    df.loc[pd.isnull(df['eligibilityEndDate']), 'eligibilityEndDate'] = pd.to_datetime(str(date.today().year) + '-12-31')
    
    msk = df['eligibilityStartDate'] > df['eligibilityEndDate']
    df = df.loc[~msk]

    print("{} members in total".format(df.shape[0]))

    elig_lst = df.values.tolist()
    members = []

    count = 0
    for e in elig_lst:
        month_range = pd.period_range(e[0], e[1], freq = 'M')
        
#         month_range = pd.date_range(e[0], e[1], freq = 'M')
        temp_lst = [[e[2], e[3], x] for x in month_range]
        members += temp_lst
        
        count += 1
        if count % 20000 == 0:
            print("{c}/{total} members finished".format(c = count, total = df.shape[0]))
    
    df_members = pd.DataFrame(members, columns = ['memberId','LOB', 'month'])
    df_members = df_members.drop_duplicates().reset_index(drop=True)
    
    return df_members


def get_tob_rev_mapping_prod():
    """
    Updated function that returns mapping_tob, mapping_rev,
    ranking,and irank from tob mapping
    @data: dataframe
    @returns: mapping_tob, mapping_rev, ranking, irank
    """
    # tob = 'typeOfBill'
    # rev_code = 'revenueCode'
    # pcat = 'providerCategoryHeader'
    # pscat = 'providerSubCategoryHeader'

    excel = 'file2'

   ###generating mapping for providerCategory using typeOfBill
    tob_mapping = pd.read_excel(excel, sheet_name='Advanced_ProviderCategory Map', dtype=str, engine=None) \
        .replace('nan', np.nan)
    tob_mapping = tob_mapping[['Type of Bill', 'providerCategory', 'provider_sub_category']]
    tob_mapping.rename(columns={'Type of Bill': 'typeOfBill',
                                'providerCategory':'providerCategoryHeader',
                                'provider_sub_category':'providerSubCategoryHeader'}, inplace=True)
    tob_mapping.loc[:, 'typeOfBill'] = tob_mapping['typeOfBill'].str.zfill(4)
    tob_mapping = tob_mapping.drop_duplicates(subset=['typeOfBill'])

    ###generating mapping for providerSubCategory using revenueCode
    rev_mapping = pd.read_excel(excel, sheet_name='Provider sub category', dtype=str, engine=None) \
        .replace('nan', np.nan)
    rev_mapping = rev_mapping[['type_of_bill', 'SUBCATEGORY_CODE', 'provider_sub_category']]
    rev_mapping.rename(columns={'type_of_bill': 'providerCategoryHeader',
                                'SUBCATEGORY_CODE': 'revenueCode',
                                'provider_sub_category': 'providerSubCategoryHeader'}, inplace=True)
    rev_mapping.loc[:, 'revenueCode'] = rev_mapping['revenueCode'].str.zfill(4)
    rev_mapping = rev_mapping.drop_duplicates(subset=['providerCategoryHeader','revenueCode'])

    # ranks
    rank = pd.read_excel(excel, sheet_name='Rank', dtype=str, engine=None)
    rank['mapCol'] = rank['providerCategoryHeader'] + rank['providerSubCategoryHeader']
    rank['mapBackCol'] = rank['providerCategoryHeader'] + rank['Rank'].astype(float).astype(str)
    ranking = dict(zip(rank['mapCol'], rank['Rank'].astype(float)))
    irank = dict(zip(rank['mapBackCol'], rank['providerSubCategoryHeader']))

    return tob_mapping, rev_mapping, ranking, irank


MAPPING_tob_prod, MAPPING_rev_prod, RANKING_new_prod, IRANK_new_prod = get_tob_rev_mapping_prod()


def add_provider_category_prod(data):
    """
    Updated function to add provider category
    """
    # If multiple TOBs, take first
    tob = 'typeOfBill'
    rev_code = 'revenueCode'
    pcat = 'providerCategoryHeader'
    pscat = 'providerSubCategoryHeader'

    data[tob] = data.groupby('claimId')[tob].transform('first')
    data.loc[data[tob] == '999', tob] = np.nan
    data = pd.merge(data,
                    MAPPING_tob_prod,
                    how='left',
                    on=tob,
                    suffixes=('Old', ''))
    data1 = data.loc[data[pscat].isnull(), :]
    data2 = data.loc[~data[pscat].isnull(), :]
    del data1[pscat]
    data1 = pd.merge(data1,
                    MAPPING_rev_prod,
                    how='left',
                    on=[pcat,rev_code],
                    suffixes=('Old', ''))
    data = pd.concat([data1, data2])
    msk = data['claimType'] == 'P'
    data.loc[msk, pcat] = 'professional'
    data.loc[msk, pscat] = 'other'
    data.loc[(~msk) & (data[pcat].isnull()), pcat] = 'other'
    data.loc[(~msk) & (data[pscat].isnull()), pscat] = 'other'
    # Now rank pscat based on pcat-pscat combo
    data['mapCol'] = (data[pcat] + data[pscat].astype(str)).map(RANKING_new_prod)
    data['maprank'] = data.groupby('claimId')['mapCol'].transform(np.min).fillna(101)
    data['mapBackCol'] = data[pcat] + data['maprank'].astype(str)

    data[pscat] = data['mapBackCol'].map(IRANK_new_prod).fillna('other')
    data.drop([x for x in data.columns if 'map' in x], axis=1, inplace=True)
    return data

def write_scorecard(output_dir,sheet_name = 'Metrics Data Quality'):
    """
    Write Multiple Metrics to one Excel file Scorecard.
    """
    writer = pd.ExcelWriter(output_dir + 'file3')

    ## Metrics Totals
    current_row = 1
    count_rows1 = metrics_totals.shape[0]

    metrics_totals.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1,
                        header = True,
                        index = False)
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), 'Metrics Total Claims')
    

    ## Year Totals       
    current_row += count_rows1 + 2
    count_rows2 = year_totals.shape[0]

    year_totals.to_excel(writer, 
                    sheet_name = sheet_name,
                    startrow = current_row,
                    startcol = 1,
                    header = True,
                    index = False)
    
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), 'Metrics Grouped by Claim Year')

    ## ClaimType & Year
    current_row += count_rows2 + 3
    count_rows3 = med_lob.shape[0]

    med_lob.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1,
                        header = True,
                        index = False)
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), 'Metrics Grouped by Medical Claim Year & LOB')

    ## Med/Rx Totals
    current_row += count_rows3 + 4
    count_rows4 = rx_lob.shape[0]

    rx_lob.to_excel(writer, 
                    sheet_name = sheet_name,
                    startrow = current_row,
                    startcol = 1,
                    header = True,
                    index = False)
    
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), 'Metrics Grouped by Rx Claim Year & LOB')
    
    # Med Provider by Year
    current_row += count_rows4 + 5
    count_rows5 = med_prov_yr.shape[0]

    med_prov_yr.to_excel(writer, 
                    sheet_name = sheet_name,
                    startrow = current_row,
                    startcol = 1,
                    header = True,
                    index = False)
    
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), 'Metrics Grouped by Medical Claim Provider Category & Year')
    
    # Med/Rx Month Totals      
    current_row += count_rows5 + 6
    count_rows6 = max(med_mon_pmpm.shape[0],rx_mon_pmpm.shape[0])

    med_mon_pmpm.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1,
                        header = True,
                        index = False)
    
    
    rx_mon_pmpm.to_excel(writer, 
                      sheet_name = sheet_name,
                      startrow = current_row,
                      startcol = 1 + med_mon_pmpm.shape[1] + 1,
                      header = True,
                      index = False)
    
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), "Medical & Rx PMPM's")
    
     # Med LOB Month Totals     
    current_row += count_rows6 + 7
    count_rows7 = med_mon_pmpm_vert.shape[0]

    med_mon_pmpm_vert.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1,
                        header = True,
                        index = False)
    
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), "Medical PMPM- Group By LOB")
    
    current_row += count_rows7 + 8
    count_rows8 = rx_mon_pmpm_vert.shape[0]
    
    rx_mon_pmpm_vert.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1,
                        header = True,
                        index = False)
    
    worksheet = writer.sheets[sheet_name]
    worksheet.merge_range('E{x}:F{y}'.format(x=current_row,y=current_row), "Rx PMPM- Group By LOB")
    
    
    current_row = 1
    count_rows8 = max(elig_metrics.shape[0], med_mm.shape[0], rx_mm.shape[0])
    elig_metrics.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1 + year_totals.shape[1] + 1,
                        header = True,
                        index = False)
    
    med_mm.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1 + year_totals.shape[1] + elig_metrics.shape[1] + 2,
                        header = True,
                        index = False)
    rx_mm.to_excel(writer, 
                        sheet_name = sheet_name,
                        startrow = current_row,
                        startcol = 1 + year_totals.shape[1] + elig_metrics.shape[1] + med_mm.shape[1] + 3,
                        header = True,
                        index = False)

    writer.save()

def timechange(timea):

    convert = time.strftime("%H:%M:%S", time.gmtime(timea))
    
    return convert


if __name__ == "__main__":

	# PATHS
	med_dir = 'path'
	med_delimiter = '|'
    
	rx_dir = 'path'
	rx_delimiter = '|'    
    
	elig_dir = 'path'
	elig_delimiter = '|'

	data_dictionay_path = 'path'
    
	output_dir = 'path'
    
    # Read Files
	print("--- Start Loading Medical Claims ---")
	starttime = time.time()

	med = pd.concat([pd.read_csv(f, 
                                 	sep=med_delimiter, 
                                 	dtype=str,
                                 	encoding = 'ISO-8859-1',
                                 	error_bad_lines = False) for f in glob.glob(med_dir + "*")])
	print("--- Med Loading Complete ---")
	print("")

	print("--- Start Loading Rx Claims ---")
	rx = pd.concat([pd.read_csv(f, 
                                	sep=rx_delimiter, 
                                	dtype=str,
                                	encoding = 'ISO-8859-1',
                                	error_bad_lines = False) for f in glob.glob(rx_dir + "*")])
	print("--- Rx Loading Complete ---")
	print("")

	print("--- Start Loading Eligibilities ---")
	elig = pd.concat([pd.read_csv(f, 
                                  	sep=elig_delimiter, 
                                  	dtype=str,
                                  	encoding = 'ISO-8859-1',
                                  	error_bad_lines = False) for f in glob.glob(elig_dir + "*")])

	print("--- Elig Loading Complete ---")
	dataendtime = time.time()
	datareadtime = timechange(dataendtime - starttime)
	print(f'Data read in {str(datareadtime)}')
	print('')

	print("--- Start Loading Data Dictionary ---")
	datadictstart = time.time()
	dd_med = pd.read_excel(data_dictionay_path, sheet_name = 'Med', dtype=str, engine='openpyxl').dropna(how = 'all')
	dd_rx = pd.read_excel(data_dictionay_path, sheet_name = 'Rx', dtype=str, engine='openpyxl').dropna(how = 'all')
	dd_elig = pd.read_excel(data_dictionay_path, sheet_name = 'Elig', dtype=str, engine='openpyxl').dropna(how = 'all')

	print("--- Loading Complete ---")

	print(f'Data dictionary ')

	# --------------------------------- Column Rename -----------------------------------------
	print("--- Preliminary Steps (0/5) Rename Columns ---")
	med = med.rename(columns = create_rename_mapper(dd_med, raw_key = 'raw fields'))\
	             .rename(columns = create_rename_mapper(dd_med, raw_key = 'raw fields substitute'))

	rx = rx.rename(columns = create_rename_mapper(dd_rx, raw_key = 'raw fields'))\
	           .rename(columns = create_rename_mapper(dd_rx, raw_key = 'raw fields substitute'))

	elig = elig.rename(columns = create_rename_mapper(dd_elig, raw_key = 'raw fields'))\
	               .rename(columns = create_rename_mapper(dd_elig, raw_key = 'raw fields substitute'))
	datadictend = time.time()   
	print('Rename Complete')
	datadicttime = timechange(datadictend - datadictstart)
	print(f'Data Dictionary Changes Completed in {str(datadicttime)}')    
	print("--- Column Renaming Complete (1/5) ---")

    # -------------------------- Change Datatype / fill na ------------------------------------
    # Med
	print("--- DataType Change (2/5) ---")
	datachangestart = time.time()    
	med['fromDate'] = pd.to_datetime(med['fromDate'], errors='coerce')
	med['paidDate'] = pd.to_datetime(med['paidDate'], errors='coerce')

	if 'serviceDate' in med.columns:
	   med['serviceDate'] = pd.to_datetime(med['serviceDate'], errors='coerce')
	else:
	   med['serviceDate'] = med['fromDate']

	med['allowedAmount'] = med['allowedAmount'].astype(float)
	med['paidAmount'] = med['paidAmount'].astype(float)
	med['lineOfBusiness'] = med['lineOfBusiness'].fillna('nan')

	# Rx
	rx['fillDate'] = pd.to_datetime(rx['fillDate'], errors='coerce')
	rx['paidDate'] = pd.to_datetime(rx['paidDate'], errors='coerce')

	if 'serviceDate' in rx.columns:
	   rx['serviceDate'] = pd.to_datetime(rx['serviceDate'], errors='coerce')
	else:
	   rx['serviceDate'] = rx['fillDate']

	rx['allowedAmount'] = rx['allowedAmount'].astype(float)
	rx['paidAmount'] = rx['paidAmount'].astype(float)
	rx['lineOfBusiness'] = rx['lineOfBusiness'].fillna('nan')

	# Elig
	elig['eligibilityStartDate'] = pd.to_datetime(elig['eligibilityStartDate'], errors='coerce')
	elig['eligibilityEndDate'] = pd.to_datetime(elig['eligibilityEndDate'], errors='coerce')

	print("--- DataType Change Complete (2/5) ---")
    

	# ----------------------- Preliminary Steps --------------------------------
    # Filter out claims before 2017
	med = med.loc[med['fromDate'] >= '2017-01-01']
	rx = rx.loc[rx['fillDate'] >= '2017-01-01']


	    ## Derive year
	med['year'] = med['fromDate'].dt.year
	rx['year'] = rx['fillDate'].dt.year
	print(med.year.unique())
	    ## Derive month
	med['month'] = med['fromDate'].dt.to_period('M')
	rx['month'] = rx['fillDate'].dt.to_period('M')
	datachangeend = time.time()  
	datachangetime = timechange(datachangeend - datachangestart)   
	print(f'Data Changes Complete {str(datachangetime)}') 
    
	    ## Derive claimtype
	claimtypestart = time.time()   
	med = add_claimtype(med)
	claimtypeend = time.time()
	claimtypetime = timechange(claimtypeend - claimtypestart)
	print(f'Add claimtype finished in {str(claimtypetime)}')
    
	print("--- Date Modification on Med & Rx Complete (3/5) ---")
	print("")

	    ## Create member months data
	print("--- Start creating Member Months data ---")
	membersstart = time.time()
	df_members = create_member_months(elig)
	df_members = df_members[df_members['month'] >= '2017-01']
	membersend = time.time()
	membertime = timechange(membersend - membersstart)
	print(f'Member Months created in {str(membertime)}')
	print("--- Creating Member Months data Complete (4/5) ---")

	print("--- Start Creating Provider Category header data (5/5) ---")
	provcatstart = time.time()
	med = add_provider_category_prod(med)
	provcatend = time.time()
	provtime = timechange(provcatend - provcatstart)
	print(f'Provider Category Header completed in {str(provtime)}')
	print("--- Provider Category header data Complete (5/5) ---")

    # ---------------------------- Group By's --------------------------------
	groupstart = time.time()
	med['lineOfBusiness'] = med['lineOfBusiness'].replace('nan', np.nan)
	df_members['LOB'] = df_members['LOB'].replace('nan', np.nan)

	df_members['year'] = df_members['month'].dt.year
	df_members['combo'] = df_members['memberId'].map(str) + df_members['month'].map(str)
	agg_members = df_members.groupby('month', as_index=False).agg({'memberId': 'nunique'})
	months = df_members.groupby('memberId').agg({'month' : 'nunique'})
	agg_member_year = df_members.groupby('year', as_index=False).agg({'combo': 'nunique'})
	agg_member_year.columns = ['Year', 'Member Months']
	agg_member_yearlob = df_members.groupby(['LOB','year'], as_index=False, dropna=False).agg({'combo': 'count'})
	agg_month = df_members.groupby('month').agg({'combo' : 'nunique'})
	lob_members = df_members.groupby(['LOB', 'month'], as_index=False, dropna=False).agg({'memberId': 'nunique'})
	groupend = time.time()
	grouptime = timechange(groupend - groupstart)
	print(f'Groupby completed in {str(grouptime)}')
 

	# ---------------------------- Metrics Totals ----------------------------
	metricsstart = time.time()
	print("--- Start Deriving Metrics ---")
	print('Metrics Total Claims')
	    
	med['Data Type'] = 'Medical'
	med['claimidline'] = med['claimId'].map(str) + med['lineNumber']
	med_totals = med.groupby('Data Type').agg({'lineNumber': 'count',
	                                    'memberId': 'nunique',
	                                    'claimId': 'nunique',
	                                    'claimidline': 'nunique',
	                                    'allowedAmount': 'sum',
	                                    'paidAmount': 'sum'})
	 
	med_totals.columns = ['Total Rows', 'Unique Claimants', 'Unique Claims', 'Unique Claim Lines', 'Allowed Amount', 'Paid Amount']

	rx['Data Type'] = 'Rx'
	rx_totals = rx.groupby('Data Type').agg({'Data Type': 'count',
	                                        'memberId': 'nunique',
	                                        'claimId': 'nunique',
	                                        'year': 'count',
	                                        'allowedAmount': 'sum',
	                                        'paidAmount': 'sum'})

	rx_totals.columns = ['Total Rows', 'Unique Claimants', 'Unique Claims', 'Unique Claim Lines', 'Allowed Amount', 'Paid Amount']

	metrics_totals = pd.concat([med_totals, rx_totals])
	metrics_totals['Member Months'] = months.month.sum()
	metrics_totals['Allowed PMPM'] = metrics_totals['Allowed Amount'] / metrics_totals['Member Months']
	metrics_totals.loc['Total']= metrics_totals.sum(numeric_only=True, axis=0)
	metrics_totals = metrics_totals.reset_index()
	metrics_totals = metrics_totals[['Data Type', 'Total Rows', 'Unique Claimants', 'Member Months', 'Unique Claims', 'Unique Claim Lines', 'Allowed Amount', 'Paid Amount', 'Allowed PMPM']]
	print('Metrics Total Claims - Complete')

	# -------------------Metrics Grouped by Claim Year--------------------------------

	print('Metrics Grouped by Claim Year')

	med_year = med.groupby(['Data Type', 'year'], as_index=False).agg({'memberId' : 'nunique',
	                                                   'claimId' : 'nunique',
	                                                   'claimidline' : 'nunique',
	                                                   'allowedAmount' : 'sum',
	                                                   'paidAmount' : 'sum'})
	rx_year = rx.groupby(['Data Type', 'year'], as_index=False).agg({'memberId' : 'nunique',
	                                                                 'claimId' : 'nunique',
	                                                                 'month' : 'count',
	                                                                 'allowedAmount' : 'sum',
	                                                                 'paidAmount' : 'sum'})

	med_year.columns = ['Data Type', 'Year', 'Unique Claimants', 'Unique Claims', 'Unique Claim Lines', 'Allowed Amount', 'Paid Amount']
	rx_year.columns = ['Data Type', 'Year', 'Unique Claimants', 'Unique Claims', 'Unique Claim Lines', 'Allowed Amount', 'Paid Amount']

	year_totals = pd.concat([med_year, rx_year])
	
	year_totals = year_totals.merge(agg_member_year, left_on='Year',right_on='Year',how='inner')
	year_totals['PMPM'] = year_totals['Allowed Amount'] / year_totals['Member Months']
	year_totals.sort_values(by=['Data Type', 'Year'], inplace=True)

	year_totals = year_totals.set_index('Data Type')
	cols = ['Unique Claimants','Unique Claims', 'Unique Claim Lines', 'Allowed Amount', 'Paid Amount', 'PMPM']
	for year in year_totals['Year'].unique():
	    year_totals.loc[f'{year} Total'] = year_totals.loc[year_totals['Year'] == year][cols].sum()
	year_totals = year_totals.reset_index()
	year_totals = year_totals[['Data Type', 'Year', 'Unique Claimants', 'Member Months', 'Unique Claims', 'Unique Claim Lines', 'Allowed Amount', 'Paid Amount', 'PMPM']]
	print('Metrics Grouped by Claim Year - Complete')

	# ---------------------Metrics: Eligibility----------------------------

	print('Metrics: Eligibility	Min/Max')
	elig['Data Type'] = 'Elig'
	elig_metrics = elig.groupby('Data Type').agg(Total_Rows=('Data Type', 'count'), Total_Unique_Members=('memberId', 'nunique'),
	                                            Minimum_Eligibility_Start_Date=('eligibilityStartDate' , 'min'), 
	                                            Maximum_Eligibility_Start_Date=('eligibilityStartDate', 'max'),
	                                            Minimum_Eligibility_End_Date=('eligibilityEndDate', 'min'),
	                                            Maximum_Eligibility_End_Date=('eligibilityEndDate', 'max'))
	    
	elig_metrics['Minimum_Eligibility_Start_Date'] = elig_metrics['Minimum_Eligibility_Start_Date'].dt.date
	elig_metrics['Maximum_Eligibility_Start_Date'] = elig_metrics['Maximum_Eligibility_Start_Date'].dt.date
	elig_metrics['Minimum_Eligibility_End_Date'] = elig_metrics['Minimum_Eligibility_End_Date'].dt.date
	elig_metrics['Maximum_Eligibility_End_Date'] = elig_metrics['Maximum_Eligibility_End_Date'].dt.date
	elig_metrics = elig_metrics.transpose()
	elig_metrics = elig_metrics.reset_index()
	elig_metrics.columns = ['Elig Metrics', ' '] 
	print('Metrics: Eligibility	Min/Max - Complete')

	# -------------------Metrics: Min/Max Date Metrics: Medical-------------------------

	print('Metrics: Min/Max Date Metrics: Medical')
	med_mm = med.groupby('Data Type').agg(Minimum_Service_Date=('fromDate', 'min'),
	                                     Maximum_Service_Date=('fromDate', 'max'),
	                                     Minimum_Paid_Date=('paidDate', 'min'),
	                                     Maximum_Paid_Date=('paidDate', 'max'))
	med_mm['Minimum_Service_Date'] = med_mm['Minimum_Service_Date'].dt.date
	med_mm['Maximum_Service_Date'] = med_mm['Maximum_Service_Date'].dt.date
	med_mm['Minimum_Paid_Date'] = med_mm['Minimum_Paid_Date'].dt.date
	med_mm['Maximum_Paid_Date'] = med_mm['Maximum_Paid_Date'].dt.date
	med_mm = med_mm.transpose()
	med_mm = med_mm.reset_index()
	med_mm.columns = ['Med Metrics', ' ']
	print('Metrics: Min/Max Date Metrics: Medical - Complete')

	# -----------------------Metrics: Min/Max Date Metrics: Rx---------------------------

	print('Metrics: Min/Max Date Metrics: Rx')
	rx_mm = rx.groupby('Data Type').agg(Minimum_Service_Date=('serviceDate', 'min'),
	                                   Maximum_Service_Date=('serviceDate', 'max'),
	                                   Minimum_Paid_Date=('paidDate', 'min'),
	                                   Maximum_Paid_Date=('paidDate', 'max'))
	rx_mm['Minimum_Service_Date'] = rx_mm['Minimum_Service_Date'].dt.date
	rx_mm['Maximum_Service_Date'] = rx_mm['Maximum_Service_Date'].dt.date
	rx_mm['Minimum_Paid_Date'] = rx_mm['Minimum_Paid_Date'].dt.date
	rx_mm['Maximum_Paid_Date'] = rx_mm['Maximum_Paid_Date'].dt.date
	rx_mm = rx_mm.transpose()
	rx_mm = rx_mm.reset_index()
	rx_mm.columns = ['Rx Metrics', ' ']
	print('Metrics: Min/Max Date Metrics: Rx - Complete')

	# --------------------- Metrics Grouped by Medical Claim Year & LOB -------------------

	print('Metrics Grouped by Medical Claim Year & LOB')
	med_lob = med.groupby(['lineOfBusiness','year'], as_index=False, dropna=False).agg({'memberId' : 'nunique',
	                                                      'claimId' : 'nunique',
	                                                      'claimidline' : 'nunique',
	                                                      'allowedAmount' : 'sum',
	                                                      'paidAmount' : 'sum'})

    # med_lob = med_lob.rename(columns={'year' : 'Year'})
	med_lob = med_lob.merge(agg_member_yearlob, left_on=['lineOfBusiness','year'],right_on=['LOB','year'], how='inner').drop('LOB',axis= 1) 

	med_lob.columns = ['LOB', 'Year', 'Unique Claimants', 'Unique Claims', 'Unique Claim Lines', 
	                   'Allowed Amount', 'Paid Amount', 'Member Months']
	med_lob['PMPM'] = med_lob['Allowed Amount'] / med_lob['Member Months']
	med_lob = med_lob.sort_values(by=['LOB', 'Year'])
	med_lob = med_lob[['LOB', 'Year', 'Unique Claimants', 'Member Months', 'Unique Claims', 'Unique Claim Lines', 
	                   'Allowed Amount', 'Paid Amount', 'PMPM']]

	# ---------------------- Metrics Grouped by Rx Claim Year & LOB ----------------------
	
	print('Metrics Grouped by Rx Claim Year & LOB')
	rx_lob = rx.groupby(['lineOfBusiness','year'], as_index=False, dropna=False).agg({'memberId' : 'nunique',
	                                                      'claimId' : 'nunique',
	                                                      'Data Type' : 'count',
	                                                      'allowedAmount' : 'sum',
	                                                      'paidAmount' : 'sum'})


	rx_lob = rx_lob.merge(agg_member_yearlob, left_on=['lineOfBusiness','year'],right_on=['LOB','year'], how='inner').drop('LOB',axis= 1)

	rx_lob.columns = ['LOB', 'Year', 'Unique Claimants', 'Unique Claims', 'Unique Claim Lines', 
	                  'Allowed Amount', 'Paid Amount', 'Member Months']
	rx_lob['PMPM'] = rx_lob['Allowed Amount'] / rx_lob['Member Months']
	rx_lob = rx_lob.sort_values(by=['LOB', 'Year'])

	rx_lob = rx_lob[['LOB', 'Year', 'Unique Claimants', 'Member Months', 'Unique Claims', 'Unique Claim Lines', 
	                   'Allowed Amount', 'Paid Amount', 'PMPM']]                   
	print('Metrics Grouped by Rx Claim Year & LOB - Complete')

	# ------------------- Metrics Grouped by Medical Claim Provider Category & Year -------------------

	print('Metrics Grouped by Medical Claim Provider Category & Year')
	med['providerCategoryHeader'] = med['providerCategoryHeader'].replace('other', 'professional')
	med_prov_yr = med.groupby(['providerCategoryHeader','year'], as_index=False).agg(Unique_Claimants=('memberId', 'nunique'),
	                                                                    Unique_Claims=('claimId', 'nunique'),
	                                                                    Unique_Claim_Lines=('claimidline', 'nunique'),
	                                                                    Allowed_Amount=('allowedAmount', 'sum'),
	                                                                    Paid_Amount=('paidAmount', 'sum'))
	med_prov_yr = med_prov_yr.rename(columns={'year' : 'Year', 'Unique_Claims' : 'Unique Claims',
	                                          'Unique_Claim_Lines' : 'Unique Claim Lines', 
	                                          'Allowed_Amount' : 'Allowed Amount',
	                                          'Paid_Amount' : 'Paid Amount',
	                                          'providerCategoryHeader' : 'Provider Category',
	                                          'Unique_Claimants' : 'Unique Claimants'})
	med_prov_yr = med_prov_yr.merge(agg_member_year, left_on='Year',right_on='Year',how='inner')

	med_prov_yr['PMPM'] = med_prov_yr['Allowed Amount'] / med_prov_yr['Member Months']
	med_prov_yr.sort_values(by=['Provider Category', 'Year'], inplace=True)

	med_prov_yr = med_prov_yr.set_index('Provider Category')
	for year in med_prov_yr['Year'].unique():
	    med_prov_yr.loc[f'{year} Total'] = med_prov_yr.loc[med_prov_yr['Year'] == year][cols].sum()
	med_prov_yr = med_prov_yr.reset_index()
	med_prov_yr = med_prov_yr[['Provider Category', 'Year', 'Unique Claimants', 'Member Months', 'Unique Claims', 
	                           'Unique Claim Lines', 'Allowed Amount', 'Paid Amount', 'PMPM']]
	print('Metrics Grouped by Medical Claim Provider Category & Year - Complete')

	# -------------------------------- Medical PMPM month -------------------------------------

	print('Medical PMPM month')

	med['month-year'] = pd.to_datetime(med['fromDate']).dt.to_period('M')
	med_mon_pmpm = med.groupby('month-year')['allowedAmount'].sum().reset_index()
	med_mon_pmpm = med_mon_pmpm.merge(agg_month, left_on='month-year', right_on='month',how='inner')
	med_mon_pmpm['Allowed PMPM'] = med_mon_pmpm['allowedAmount'] / med_mon_pmpm['combo']
	med_mon_pmpm.columns = ['Month/Year', 'Allowed Amount', 'Member Months', 'Allowed PMPM']
	med_mon_pmpm['PMPM Monthly Change'] = med_mon_pmpm['Allowed PMPM'].pct_change() * 100
	med_mon_pmpm['PMPM Monthly Change'] = med_mon_pmpm['PMPM Monthly Change'].replace(np.nan, 0).round(3).astype(str) + '%'


	#----------------------------------- Rx PMPM month ---------------------------------------

	rx['month-year'] = pd.to_datetime(rx['fillDate']).dt.to_period('M')
	rx_mon_pmpm = rx.groupby('month-year')['allowedAmount'].sum().reset_index()
	rx_mon_pmpm
	rx_mon_pmpm = rx_mon_pmpm.merge(agg_month, left_on='month-year', right_on='month',how='inner')
	rx_mon_pmpm['Allowed PMPM'] = rx_mon_pmpm['allowedAmount'] / rx_mon_pmpm['combo']
	rx_mon_pmpm.columns = ['Month/Year', 'Allowed Amount', 'Member Months', 'Allowed PMPM']
	rx_mon_pmpm['PMPM Monthly Change'] = rx_mon_pmpm['Allowed PMPM'].pct_change() * 100
	rx_mon_pmpm['PMPM Monthly Change'] = rx_mon_pmpm['PMPM Monthly Change'].replace(np.nan, 0).round(3).astype(str) + '%'
	print('Medical PMPM month - Complete')

	# --------------------------- Medical PMPM- Group By LOB -----------------------------
    # Medical PMPM- Group By LOB
	print('Medical PMPM- Group By LOB')
	med_mon_pmpm_vert = med.groupby(['lineOfBusiness','month'], as_index=False, dropna=False).agg({'allowedAmount' : 'sum'})
	med_mon_pmpm_vert = med_mon_pmpm_vert.merge(lob_members, left_on=['lineOfBusiness','month'], right_on=['LOB','month'],how='left')

	med_mon_pmpm_vert = med_mon_pmpm_vert.drop(['LOB'],axis=1)
	med_mon_pmpm_vert.columns = ['lineOfBusiness', 'Month/Year', 'Allowed Amount', 'Member Months']

	med_mon_pmpm_vert['Allowed PMPM'] = med_mon_pmpm_vert['Allowed Amount'] / med_mon_pmpm_vert['Member Months']
	med_mon_pmpm_vert.sort_values(by=['lineOfBusiness', 'Month/Year'], inplace=True)
	med_mon_pmpm_vert = med_mon_pmpm_vert.reset_index(drop=True)
	med_mon_pmpm_vert['pct_change'] = (med_mon_pmpm_vert.groupby('lineOfBusiness', dropna=False)['Allowed PMPM']
	                                  .apply(pd.Series.pct_change) ).fillna(0).round(3)
	med_mon_pmpm_vert['PMPM Monthly Change < 10%'] = np.where(med_mon_pmpm_vert['pct_change'] < 0.10, False, True)
	print('Medical PMPM- Group By LOB - Complete')
    

	# -------------------------- Rx PMPM- Group By LOB ----------------------------------

	print('Rx PMPM- Group By LOB')

	rx_mon_pmpm_vert = rx.groupby(['lineOfBusiness','month'], as_index=False).agg({'allowedAmount' : 'sum'})
	
	rx_mon_pmpm_vert = rx_mon_pmpm_vert.merge(lob_members, left_on=['lineOfBusiness','month'], right_on=['LOB','month'],how='left')
	
	rx_mon_pmpm_vert = rx_mon_pmpm_vert.drop(['LOB'],axis=1)
	rx_mon_pmpm_vert.columns = ['LOB', 'Month/Year', 'Allowed Amount', 'Member Months']
	rx_mon_pmpm_vert['Allowed PMPM'] = rx_mon_pmpm_vert['Allowed Amount'] / rx_mon_pmpm_vert['Member Months']

	rx_mon_pmpm_vert.sort_values(by=['LOB', 'Month/Year'], inplace=True)
	rx_mon_pmpm_vert = rx_mon_pmpm_vert.reset_index(drop=True)
	rx_mon_pmpm_vert['pct_change'] = (rx_mon_pmpm_vert.groupby('LOB')['Allowed PMPM']
	                                  .apply(pd.Series.pct_change) ).fillna(0).round(3)
	rx_mon_pmpm_vert['PMPM Monthly Change < 10%'] = np.where(rx_mon_pmpm_vert['pct_change'] < 0.10, True, False)
	print('Rx PMPM- Group By LOB - Complete')
	metricsend = time.time()
	metricstime = timechange(metricsend - metricsstart)
	print(f'Metrics done in {str(metricstime)}')

	# -------------------------------- 	 WRITE SCORECARD   --------------------------------
	print('Writing Scorecard')
	Path(output_dir).mkdir(parents=True, exist_ok=True)
	write_scorecard(output_dir = output_dir)
	metrics_totals.to_csv(output_dir+'metrics_totals_writetest.csv', index=False)
	year_totals.to_csv(output_dir+'year_totals_writetest.csv', index=False)
	med_lob.to_csv(output_dir+'med_lob_writetest.csv', index=False)
	rx_lob.to_csv(output_dir+'rx_lob_writetest.csv', index=False)
	med_prov_yr.to_csv(output_dir+'med_prov_yr_writetest.csv', index=False)
	med_mon_pmpm.to_csv(output_dir+'med_mon_pmpm_writetest.csv', index=False)
	rx_mon_pmpm.to_csv(output_dir+'rx_mon_pmpm_writetest.csv', index=False)
	med_mon_pmpm_vert.to_csv(output_dir+'med_mon_pmpm_vert_writetest.csv', index=False)
	rx_mon_pmpm_vert.to_csv(output_dir+'rx_mon_pmpm_vert_writetest.csv', index=False)
	elig_metrics.to_csv(output_dir+'elig_metrics_writetest.csv', index=False)
	med_mm.to_csv(output_dir+'med_mm_writetest.csv', index=False)
	rx_mm.to_csv(output_dir+'rx_mm_writetest.csv', index=False)
    
	endtime = time.time()
	totaltime = timechange(endtime - starttime)
	print(f'Script finished running in: {totaltime}')








