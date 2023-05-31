# Accounting

This folder contains several files for accounting.

* New_getFileForBanana_polygon.py  
This python code takes all SCL transactions from etherscan API and adds the corresponding commitmentID to each row.
If a row has a -1 as commitmentID it means that the transaction either failed orthat the transaction hasn't anything
to do with the order process (e.g. a new Commitment).
By running the code the SCL Orders and Delivery Database are updates and two new .csv files are produces.

    - **SCL_Orders_Database_up_to_block_#blocknumber.csv**: contains all relevant information for the Receivers 
    - **SCL_Delivery_Database_up_to_block_#blocknumber.csv**: contains all relevant information for the Senders   
    - **SCL_block_#blocknumber_to_#blocknumber.csv**: are the files to be copied to Banana Software for accounting.

