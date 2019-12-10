import datetime, os
import pandas as pd

# MANUAL STEPS:
# 1. Visit https://enforcedata.dol.gov/views/data_catalogs.php and click MSAH data 
# 2. Download and unzip msha_cy_oprtr_emplymnt
# 3. Download and unzip msha_mine_zip
# These files are updated weekly.


# VARIABLES TO CHANGE DEPENDING ON DATE FILES DOWNLOADED AND STATES DESIRED
col_types = {'mine_id':str}
region_filter = ['KY', 'OH', 'WV']
#msha_data_root = '/absolute/path/to/your/downloaded/msha/data/'
msha_data_root = '/Users/akanik/LPM/data/msha-data/msha-state-emp-prod/data/'

# OTHER VARIABLES
today = datetime.datetime.now()
process_date = today.strftime('%Y%m%d')
msha_cy_oprtr_emplymnt = msha_data_root + 'msha_cy_oprtr_emplymnt_20191130-0.csv'
msha_mine = msha_data_root + 'msha_mine_20191130-0.csv'

mine_data = pd.read_csv(msha_mine, escapechar='\\', dtype=col_types)
oprtr_data = pd.read_csv(msha_cy_oprtr_emplymnt, dtype=col_types)


def get_state_coal_mines(state_list):
    coal = mine_data['c_m_ind'] == 'C'
    regional = mine_data['state_abbr'].isin(state_list)
    
    if (len(state_list) == 1) and ('US' in state_list):
        filtered_coal_mines = mine_data.loc[coal]
        filtered_coal_unique = filtered_coal_mines.drop_duplicates(subset='mine_id')
        return filtered_coal_unique['mine_id'].tolist()
    else:
        filtered_coal_mines = mine_data.loc[(coal) & (regional)]
        filtered_coal_unique = filtered_coal_mines.drop_duplicates(subset='mine_id')
        return filtered_coal_unique['mine_id'].tolist()
            

# This will filter each state in your region_filter list into a new csv file
def filter_state_operators(state_list):
    # create a 'filtered' directory if it doesn't already exist
    directory = msha_data_root + 'filtered/'
    
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # run each state in your region_filter list separately 
    for state in state_list:
        print('Starting on ' + state)
        
        mine_operators_new_file = directory + state + '_oprtr_emplymnt_' + process_date + '.csv'
        # create a single-item list out of our state so we can run it through get_state_coal_mines()
        state_as_list = [state]
        # return a list of coal mines ids that are in the state we're currently processing
        filtered_coal_mines = get_state_coal_mines(state_as_list)
        
        print('- returned', len(filtered_coal_mines), state, 'mines')
        
        oprtr_data_filtered = oprtr_data.loc[oprtr_data['mine_id'].isin(filtered_coal_mines)]
        oprtr_data_filtered.to_csv(mine_operators_new_file)
                
        
# This will filter all of the state in your region_filter list into a single csv file   
def filter_regional_operators(state_list):
    # create a 'filtered' directory if it doesn't already exist
    directory = msha_data_root + 'filtered/'
    if not os.path.exists(directory):
        os.makedirs(directory)
        
    # name our output file
    region_str = '_'.join(state_list)
    mine_operators_new_file = directory + region_str + '_oprtr_emplymnt_' + process_date + '.csv'
    
    print('Starting on region ' + region_str)
    
    # return a list of coal mine ids that are in one of the states in our region_filter
    filtered_coal_mines = get_state_coal_mines(state_list)
    
    print('- returned', len(filtered_coal_mines), region_str, 'mines')
    
    oprtr_data_filtered = oprtr_data.loc[oprtr_data['mine_id'].isin(filtered_coal_mines)]
    oprtr_data_filtered.to_csv(mine_operators_new_file)

        
def create_employment_production():
    # create a 'by_year' directory if it doesn't already exist
    directory = msha_data_root + 'filtered/by_year/'
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # perform this on all the csv files we just created in our filtered directory
    for file in os.listdir(msha_data_root + 'filtered/'):
        if file.endswith('.csv'):
            print('Aggregating employment and production for ' + file)
            
            regional_oprtr = pd.read_csv(msha_data_root + 'filtered/' + file)
            # Pivot on the year
            op_by_year = regional_oprtr.groupby('calendar_yr').agg({'avg_annual_empl':'sum',
                                                                    'annual_coal_prod':'sum'})
            op_by_year.sort_values('calendar_yr').to_csv(directory + file)
            
                     
filter_state_operators(region_filter)
filter_regional_operators(region_filter)
filter_regional_operators(['US'])
create_employment_production()