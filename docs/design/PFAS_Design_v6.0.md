Personal Financial Accounting System

**Technical Design Document**

**Version 6.0**

January 2026

1\. Architecture Overview

1.1 System Architecture

PFAS follows a layered architecture with clear separation of concerns:

1.  **Presentation Layer:** CLI interface, future web UI

2.  **Service Layer:** Business logic, tax calculations, validations

3.  **Parser Layer:** Document parsing, data extraction

4.  **Data Access Layer:** SQLite with SQLCipher encryption

5.  **Integration Layer:** Export to GNUCash, ITR JSON generation

1.2 Technology Stack

  ------------------- ----------------------- ---------------------------
  **Component**       **Technology**          **Purpose**

  Runtime             Python 3.11+            Core application runtime

  Database            SQLite + SQLCipher      Encrypted local storage

  PDF Parsing         PyPDF2, pdfplumber,     Document extraction
                      tabula-py               

  Excel Parsing       openpyxl, pandas        Spreadsheet processing

  Encryption          cryptography (AES-256)  Field-level encryption

  CLI                 Click, Rich             Command-line interface

  Testing             pytest, coverage        Unit and integration tests
  ------------------- ----------------------- ---------------------------

2\. Database Schema

2.1 Core Tables

  --------------------- ---------------------------------------- -------------
  **Table**             **Columns**                              **Purpose**

  **users**             id, name, pan, aadhaar_enc, email,       User master
                        password_hash, created_at                

  **accounts**          id, code, name, parent_id, type,         Chart of
                        asset_class, gnucash_map                 accounts

  **journals**          id, date, narration, ref_type, ref_id,   Journal
                        created_by, created_at                   headers

  **journal_entries**   id, journal_id, account_id, debit,       Journal lines
                        credit, currency, fx_rate                

  **audit_log**         id, user_id, action, table_name,         Audit trail
                        record_id, old_value, new_value,         
                        timestamp                                
  --------------------- ---------------------------------------- -------------

2.2 Bank Tables

  ----------------------- ---------------------------------------- --------------
  **Table**               **Columns**                              **Purpose**

  **bank_accounts**       id, user_id, bank_name, account_no_enc,  Bank account
                          ifsc, account_type                       master

  **bank_transactions**   id, bank_account_id, txn_date,           Bank
                          narration, debit, credit, balance,       transactions
                          category                                 
  ----------------------- ---------------------------------------- --------------

2.3 Mutual Fund Tables

  ---------------------- ---------------------------------------- --------------
  **Table**              **Columns**                              **Purpose**

  **mf_folios**          id, user_id, amc_name, folio_no,         MF folio
                         scheme_name, isin, asset_class           master

  **mf_transactions**    id, folio_id, txn_type, txn_date, units, MF
                         nav, amount, stt                         transactions

  **mf_capital_gains**   id, sale_txn_id, purchase_txn_id,        MF capital
                         holding_days, cost, proceeds, gain_type, gains
                         gain                                     
  ---------------------- ---------------------------------------- --------------

2.4 Stock Tables

  ------------------------- ---------------------------------------- -------------
  **Table**                 **Columns**                              **Purpose**

  **stock_holdings**        id, user_id, broker, symbol, isin,       Stock
                            quantity, avg_cost                       holdings

  **stock_trades**          id, holding_id, trade_type, trade_date,  Stock trades
                            quantity, price, brokerage, stt          

  **stock_dividends**       id, holding_id, record_date, amount, tds Dividend
                                                                     records

  **stock_capital_gains**   id, sale_trade_id, purchase_trade_id,    Stock CG
                            holding_days, cost, proceeds, gain_type, 
                            gain                                     
  ------------------------- ---------------------------------------- -------------

2.5 Retirement Fund Tables

  ---------------------- ---------------------------------------- --------------
  **Table**              **Columns**                              **Purpose**

  **epf_accounts**       id, user_id, uan, member_id,             EPF account
                         establishment_id, establishment_name     

  **epf_transactions**   id, epf_account_id, wage_month,          EPF
                         txn_date, ee_epf, er_epf, pension, wage  transactions

  **ppf_accounts**       id, user_id, account_no_enc, bank_name,  PPF account
                         open_date, maturity_date                 

  **ppf_transactions**   id, ppf_account_id, txn_date, deposit,   PPF
                         interest, balance                        transactions

  **nps_accounts**       id, user_id, pran, tier, fund_manager    NPS account

  **nps_transactions**   id, nps_account_id, txn_date,            NPS
                         contribution_type, amount, units, nav    transactions
  ---------------------- ---------------------------------------- --------------

2.6 Salary & Tax Tables

  ----------------------- ---------------------------------------- -------------
  **Table**               **Columns**                              **Purpose**

  **employers**           id, name, pan, tan, address              Employer
                                                                   master

  **salary_records**      id, user_id, employer_id, month, basic,  Monthly
                          hra, special_allowance, lta,             salary
                          total_earnings                           

  **salary_deductions**   id, salary_id, component, amount,        Salary
                          is_tax_saving                            deductions

  **rsu_tax_credits**     id, salary_id, amount, vest_date,        RSU tax
                          shares_vested                            credits

  **form16_records**      id, user_id, employer_id, fy,            Form 16 data
                          part_a_json, part_b_json                 
  ----------------------- ---------------------------------------- -------------

2.7 Income Tax Tables

  -------------------------- ---------------------------------------- ----------------
  **Table**                  **Columns**                              **Purpose**

  **form26as_headers**       id, user_id, pan, fy, ay,                26AS header
                             data_updated_date                        

  **form26as_tds**           id, header_id, deductor_name, tan,       26AS TDS
                             section, txn_date, amount_paid,          
                             tax_deducted, status                     

  **form26as_tcs**           id, header_id, collector_name, tan,      26AS TCS
                             section, txn_date, amount_debited,       
                             tax_collected                            

  **advance_tax_payments**   id, user_id, ay, cin, payment_date,      Advance tax
                             amount, bank_name                        

  **tds_reconciliation**     id, user_id, fy, source_type,            TDS
                             source_ref, form26as_ref, variance,      reconciliation
                             status                                   
  -------------------------- ---------------------------------------- ----------------

2.8 Rental Income Tables

  ------------------------ ---------------------------------------- -------------
  **Table**                **Columns**                              **Purpose**

  **properties**           id, user_id, address, property_type,     Property
                           municipal_value, loan_account_id         master

  **rental_income**        id, property_id, month, gross_rent,      Rental income
                           municipal_tax, std_deduction, net_income 

  **home_loan_interest**   id, property_id, fy, principal_paid,     Home loan
                           interest_paid, sec24_deduction           interest
  ------------------------ ---------------------------------------- -------------

2.9 SGB/REIT Tables

  ------------------------ ---------------------------------------- ---------------
  **Table**                **Columns**                              **Purpose**

  **sgb_holdings**         id, user_id, series, units, issue_date,  SGB holdings
                           maturity_date, issue_price               

  **sgb_interest**         id, sgb_id, credit_date, amount          SGB interest

  **reit_holdings**        id, user_id, symbol, isin, units,        REIT holdings
                           cost_basis                               

  **reit_distributions**   id, reit_id, record_date, dividend,      REIT
                           interest, other_income,                  distributions
                           capital_reduction                        
  ------------------------ ---------------------------------------- ---------------

2.10 Foreign Asset Tables

  ------------------------- ---------------------------------------- -------------
  **Table**                 **Columns**                              **Purpose**

  **foreign_brokers**       id, user_id, broker_name,                Foreign
                            account_no_enc, country_code             broker

  **rsu_grants**            id, user_id, grant_number, grant_date,   RSU grants
                            total_shares, symbol                     

  **rsu_vests**             id, grant_id, vest_date, shares_vested,  RSU vests
                            fmv_usd, fmv_inr, tt_rate                

  **rsu_sales**             id, vest_id, sale_date, shares_sold,     RSU sales
                            proceeds_usd, proceeds_inr, cost_basis,  
                            gain                                     

  **espp_purchases**        id, user_id, purchase_date, shares,      ESPP
                            market_price, purchase_price,            purchases
                            discount_value, tt_rate                  

  **espp_sales**            id, espp_id, sale_date, shares_sold,     ESPP sales
                            proceeds_usd, proceeds_inr, cost_basis,  
                            gain                                     

  **drip_records**          id, user_id, dividend_date,              DRIP records
                            dividend_usd, shares_purchased,          
                            price_per_share                          

  **foreign_tax_credits**   id, user_id, fy, country, income_type,   DTAA credits
                            foreign_income, foreign_tax, dtaa_credit 

  **schedule_fa**           id, user_id, fy, asset_type, country,    Schedule FA
                            account_details, peak_balance, income    
  ------------------------- ---------------------------------------- -------------

2.11 Unlisted Share Tables

  ------------------------- ---------------------------------------- -------------
  **Table**                 **Columns**                              **Purpose**

  **unlisted_shares**       id, user_id, company_name, company_pan,  Unlisted
                            shares, face_value, purchase_date,       holdings
                            purchase_price                           

  **unlisted_valuations**   id, unlisted_id, valuation_date,         FMV
                            fmv_per_share, valuation_method          valuations

  **unlisted_sales**        id, unlisted_id, sale_date, shares_sold, Unlisted
                            sale_price, cost_basis, gain             sales
  ------------------------- ---------------------------------------- -------------

3\. Parser Components

  ---------------------------- ------------ --------------- ------------------------
  **Parser**                   **Input**    **Libraries**   **Pattern**

  **BankStatementParser**      PDF/Excel    PyPDF2,         Regex for debit/credit
                                            openpyxl        lines

  **CAMSCASParser**            PDF          PyPDF2 with     MF transaction pattern
                               (password)   decryption      matching

  **KARVYParser**              Excel        openpyxl        Sheet-wise data
                                                            extraction

  **ICICIDirectParser**        CSV/Excel    pandas          Trade P&L column mapping

  **ZerodhaParser**            Excel        openpyxl        Multi-sheet extraction

  **EPFPassbookParser**        PDF          PyPDF2, tabula  Table extraction with
                                                            Hindi headers

  **PPFStatementParser**       Excel        openpyxl        Transaction row
                                                            extraction

  **NPSStatementParser**       CSV          pandas          Standard CSV parsing

  **PayslipParser**            PDF          PyPDF2,         Key-value pair
                                            pdfplumber      extraction

  **Form16Parser**             ZIP/PDF      zipfile, PyPDF2 Archive extraction, TXT
                                                            parsing

  **Form26ASParser**           ZIP/PDF      zipfile, PyPDF2 Archive extraction,
                                                            section parsing

  **ChallanParser**            PDF          PyPDF2          CIN, amount extraction

  **ETRADEParser**             PDF/Excel    tabula,         Stock plan statement
                                            openpyxl        parsing

  **MorganStanleyParser**      PDF          pdfplumber      Client statement parsing

  **SGBParser**                PDF          PyPDF2          NSDL CAS pattern

  **REITDistributionParser**   PDF          PyPDF2          Distribution breakdown
  ---------------------------- ------------ --------------- ------------------------

4\. Journal Entry Templates

4.1 Salary Journal

**Monthly Salary Credit:**

Dr. 1101 Bank Account ₹X,XX,XXX (Net Pay)

Dr. 5200 TDS Receivable ₹X,XX,XXX (Income Tax)

Dr. 1301 EPF Investment ₹X,XX,XXX (EE PF)

Dr. 1305 NPS Investment ₹X,XX,XXX (NPS)

Cr. 4100 Salary Income ₹X,XX,XXX (Gross)

4.2 RSU Vest Journal

**RSU Vest Event:**

Dr. 1401 Foreign Stocks (RSU) ₹X,XX,XXX (FMV at vest)

Cr. 4100 Salary Income ₹X,XX,XXX (Perquisite)

4.3 MF Redemption Journal

**MF Redemption with LTCG:**

Dr. 1101 Bank Account ₹X,XX,XXX (Proceeds)

Cr. 1201 MF Investment ₹X,XX,XXX (Cost)

Cr. 4300 LTCG Income ₹X,XX,XXX (Gain)

4.4 Rental Income Journal

**Monthly Rent:**

Dr. 1101 Bank Account ₹X,XX,XXX (Net Rent)

Cr. 4250 Rental Income ₹X,XX,XXX (Gross)

Cr. 2300 Municipal Tax Payable ₹X,XXX (If due)

5\. GNUCash Account Mapping

  ---------- --------------------------- -------------------------------------
  **Code**   **PFAS Account**            **GNUCash Path**

  **1101**   Bank - Savings              Assets:Current Assets:Bank:Savings

  **1201**   Mutual Fund - Equity        Assets:Investments:Mutual
                                         Funds:Equity

  **1202**   Mutual Fund - Debt          Assets:Investments:Mutual Funds:Debt

  **1301**   EPF Investment              Assets:Investments:Retirement:EPF

  **1302**   PPF Investment              Assets:Investments:Retirement:PPF

  **1303**   NPS Investment              Assets:Investments:Retirement:NPS

  **1310**   Indian Stocks               Assets:Investments:Equity:Indian
                                         Stocks

  **1320**   SGB Holdings                Assets:Investments:Bonds:SGB

  **1330**   RBI Bonds                   Assets:Investments:Bonds:RBI Floating
                                         Rate

  **1340**   REIT Holdings               Assets:Investments:REIT

  **1401**   Foreign Stocks - RSU        Assets:Investments:Foreign:USA:RSU

  **1402**   Foreign Stocks - ESPP       Assets:Investments:Foreign:USA:ESPP

  **4100**   Salary Income               Income:Salary:Gross Salary

  **4201**   Bank Interest               Income:Interest:Bank Interest

  **4250**   Rental Income               Income:Rental Income:Property

  **4300**   Capital Gains               Income:Capital Gains

  **5200**   TDS Receivable              Assets:Tax:TDS Receivable

  **5210**   Foreign Tax Credit          Assets:Tax Credits:Foreign Tax Credit
  ---------- --------------------------- -------------------------------------

6\. Security Design

6.1 Encryption Strategy

  ------------------- ------------------- -------------------------------
  **Data Type**       **Method**          **Implementation**

  Database            SQLCipher           Full database encryption with
                                          AES-256

  PAN/Aadhaar         AES-256-GCM         Field-level encryption with
                                          unique salt per record

  Bank Account No     AES-256-GCM         Encrypted with user-specific
                                          key

  PDF Passwords       OS Keyring          Stored in system credential
                                          manager
  ------------------- ------------------- -------------------------------

6.2 Access Control

6.  Application PIN/Password required at startup

7.  Session timeout after 15 minutes of inactivity

8.  Re-authentication required for exports

9.  All operations logged to audit trail

*\-\-- End of Design Document v6.0 \-\--*
