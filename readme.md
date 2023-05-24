# Accounting

This folder contains stuff for accounting.

* getFileForBanana.py  
This python code takes all BCP transactions from etherscan API and adds the corresponding commitmentID to each row.
If a row has a -1 as commitmentID it means that the transaction either failed orthat the transaction hasn't anything
to do with the order process (e.g. a new Commitment).
By running the code the BCP Orders and Delivery Database are updates and two new .csv files are produces.

    - **BCP_Orders_Database_up_to_block_#blocknumber.csv**: contains all relevant information for the Receivers 
    - **BCP_Delivery_Database_up_to_block_#blocknumber.csv**: contains all relevant information for the Senders   
    - **BCP_block_#blocknumber_to_#blocknumber.csv**: are the files to be copied to Banana Software for accounting.
    - **exchangeRate_#blocknumber_to_#blocknumber.csv**: list all eth/chf exchange rates on the days when BCP-transactioons were executed
