# msha-state-emp-prod
A script to filter and aggregate MSHA state coal mine employment and production

## Steps
I'm still working on automizing the download and unzip process but I've have had trouble with downloading the files from MSHA.

1. Visit [https://ogesdw.dol.gov/views/data_summary.php and click MSAH data](https://ogesdw.dol.gov/views/data_summary.php) and click MSAH data.
2. Download and unzip **msha_cy_oprtr_emplymnt** in the data directory (These files are updated weekly)
3. Download and unzip **msha_mine_zip** in the data directory (These files are updated weekly)
4. Set the `region_filter` and `msha_data_root` according to your needs/setup
5. Run the `requirements.txt` file to install agate if not already installed
6. Run the `process-msha.py` file

That should be all you need. You will be able to view state and/or regionally filtered `msha_cy_oprtr_emplymnt` files here:
`path/to/your/downloaded/msha/data/filtered` and the employment and production numbers, by year, here: `path/to/your/downloaded/msha/data/filtered/by_year`
