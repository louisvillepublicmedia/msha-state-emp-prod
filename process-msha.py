import csv, datetime, os, agate

# MANUAL STEPS:
# 1. Visit https://ogesdw.dol.gov/views/data_summary.php and click MSAH data 
# 2. Download and unzip msha_cy_oprtr_emplymnt
# 3. Download and unzip msha_mine_zip
# These files are updated weekly.


# VARIABLES TO CHANGE DEPENDING ON DATE FILES DOWNLOADED AND STATES DESIRED
region_filter = ['KY', 'OH', 'WV']
#msha_data_root = '/where/you/download/msha/data/'
msha_data_root = '/Users/akanik/LPM/data/msha-data/data/'

# VARIABLES
today = datetime.datetime.now()
process_date = today.strftime('%Y%m%d')
msha_cy_oprtr_emplymnt = msha_data_root + 'msha_cy_oprtr_emplymnt.csv'
msha_mine = msha_data_root + 'msha_mine.csv'


def get_state_coal_mines(state_list):
    
    # Create a list to hold mine_id that come from one of the states in our state_list
    filtered_coal_mines = []
    with open(msha_mine) as mf:
        mf_reader = csv.DictReader(mf)
        
        for row in mf_reader:
            # If the mine is in one of our states and it is listed as a coal mine
            # add it to our filtered_coal_mines list
            if row['state_abbr'] in state_list and row['c_m_ind'] == 'C':
                if row['mine_id'] in filtered_coal_mines:
                    pass
                else:
                    filtered_coal_mines.append(row['mine_id'])
    return filtered_coal_mines
    
    

# This will filter each state in your region_filter list into a new csv file
def filter_individual_state_operator_data(state_list):
    
    # create a 'filtered' directory if it doesn't already exist
    directory = msha_data_root + 'filtered/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # run each state in your region_filter list separately 
    for state in state_list:
        print 'Starting on ' + state
        mine_operators_new_file = directory + state + '_oprtr_emplymnt_' + process_date + '.csv'
        # create a single-item list out of our state so we can run it through get_state_coal_mines()
        state_as_list = [state]
        
        # return a list of coal mines ids that are in the state we're currently processing
        filtered_coal_mines = get_state_coal_mines(state_as_list)
        
        with open(mine_operators_new_file, 'w') as new_oef:
            with open(msha_cy_oprtr_emplymnt) as oef:
                oef_reader = csv.DictReader(oef)
                oef_header = oef_reader.fieldnames
                new_oef_writer = csv.DictWriter(new_oef, fieldnames=oef_header)

                new_oef_writer.writeheader()
                
                # go through the downloaded msha_cy_oprtr_emplymnt file and write rows to
                # a new csv, only if the mine_id of that row is from the state we're currently
                # processing
                for row in oef_reader:
                    if row['mine_id'] in filtered_coal_mines:
                        new_oef_writer.writerow(row)
                        
        new_oef.close()
        
        
        
# This will filter all of the state in your region_filter list into a single csv file   
def filter_regional_operator_data(state_list):
    
    # create a 'filtered' directory if it doesn't already exist
    directory = msha_data_root + 'filtered/'
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    # name our output file
    region_str = '_'.join(state_list)
    mine_operators_new_file = directory + region_str + '_oprtr_emplymnt_' + process_date + '.csv'
    
    print 'Starting on region ' + region_str
    
    # return a list of coal mine ids that are in one of the states in our region_filter
    filtered_coal_mines = get_state_coal_mines(state_list)
            
    with open(mine_operators_new_file, 'w') as new_oef:
        with open(msha_cy_oprtr_emplymnt) as oef:
            oef_reader = csv.DictReader(oef)
            oef_header = oef_reader.fieldnames
            new_oef_writer = csv.DictWriter(new_oef, fieldnames=oef_header)
            
            new_oef_writer.writeheader()
            
            # go through the downloaded msha_cy_oprtr_emplymnt file and write rows to
            # a new csv, only if the mine_id is in our region
            for row in oef_reader:
                if row['mine_id'] in filtered_coal_mines:
                    new_oef_writer.writerow(row)
                    
    new_oef.close()
    
    

        
def create_employment_production():
    
    # create a 'by_year' directory if it doesn't already exist
    directory = msha_data_root + 'filtered/by_year/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # perform this on all the csv files we just created in our filtered directory
    for file in os.listdir(msha_data_root + 'filtered/'):
        if file.endswith('.csv'):
            print 'Aggregating employment and production for ' + file
            
            operator_file = agate.Table.from_csv(msha_data_root + 'filtered/' + file)
            # Pivot on the year
            op_by_year = operator_file.group_by('calendar_yr')
            # sum the average annual employment and annual coal production columns 
            op_by_year_count = op_by_year.aggregate([
                ('employee_cnt', agate.Sum('avg_annual_empl')),
                ('production_cnt', agate.Sum('annual_coal_prod'))
            ])
            # sort by the year column and send to a csv
            op_by_year_count.order_by('calendar_yr').to_csv(directory + file)
            
            
            
filter_individual_state_operator_data(region_filter)
filter_regional_operator_data(region_filter)
create_employment_production()

    
