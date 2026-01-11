Personal Financial Accounting System

**Consolidated Requirements Specification**

**Version 6.0**

January 2026

*Complete Feature Set with Unique Requirement IDs*

Document Version History

  ------------- ----------- --------------------------------------------------
  **Version**   **Date**    **Changes**

  1.0           Jan 2026    Initial: Bank/MF/Stock parsers, double-entry
                            bookkeeping

  2.0           Jan 2026    Added 14 asset classes, Google Finance, GNUCash
                            compatibility

  3.0           Jan 2026    Added EPF/PPF/NPS parsers, retirement planning,
                            Phase 1/2 split

  4.0           Jan 2026    Added E\*TRADE, RSU/ESPP/DRIP, SBI TT rates,
                            Schedule FA

  5.0           Jan 2026    Salary/Form16 parser, Rental income, DTAA credit,
                            GNUCash export

  5.1           Jan 2026    Form 26AS/Form 16/Form 12BA parsing, TDS/TCS
                            reconciliation

  **6.0**       **Jan       **Consolidated requirements with unique IDs,
                2026**      Input/Process/Output specs, Test coverage matrix**
  ------------- ----------- --------------------------------------------------

1\. Executive Summary

PFAS v6.0 is a comprehensive personal financial accounting system for
Indian tax residents. This consolidated specification defines **118
unique requirements** across **18 asset classes**, organized into 18
functional categories with complete Input/Process/Output specifications.

1.1 Asset Classes (18 Total)

  -------- ------------------------ ------------------------------- -----------
  **\#**   **Asset Class**          **Tax Treatment**               **Phase**

  1        Bank Accounts            Interest: Slab, 80TTA/80TTB     Phase 1

  2        Mutual Funds - Equity    STCG 20%, LTCG 12.5%            Phase 1

  3        Mutual Funds - Debt      Slab rate (post Apr 2023)       Phase 1

  4        Indian Stocks            STCG 20%, LTCG 12.5%            Phase 1

  5        EPF                      EEE, taxable if \>2.5L interest Phase 1

  6        PPF                      EEE - Fully exempt              Phase 1

  7        NPS                      80CCD, 60% tax-free withdrawal  Phase 1

  8        Fixed Deposits           Interest: Slab, TDS             Phase 1

  9        Recurring Deposits       Interest: Slab                  Phase 1

  10       Salary Income            Slab, HRA/LTA exemptions        Phase 1

  11       Rental Income            30% SD, Sec 24 interest         Phase 1

  12       SGB (Sovereign Gold      Interest: Slab, CG exempt at    Phase 1
           Bonds)                   maturity                        

  13       RBI Floating Rate Bonds  Interest: Slab (floating)       Phase 1

  14       REITs/InvITs             Split: Dividend/Interest/CG     Phase 1

  15       USA Stocks - RSU         Perquisite + CG (24mo)          Phase 2

  16       USA Stocks - ESPP        Discount perquisite + CG        Phase 2

  17       USA Stocks - DRIP        Dividend income + CG            Phase 2

  18       Unlisted Shares          24mo LTCG, valuation rules      Phase 2
  -------- ------------------------ ------------------------------- -----------

1.2 Requirements Summary

  ------------------- ---------------- ----------- ---------------------------
  **Category**        **ID Range**     **Count**   **Description**

  REQ-CORE            REQ-CORE-001 -   7           Core system, database,
                      007                          security

  REQ-BANK            REQ-BANK-001 -   4           Bank statement parsing,
                      004                          interest

  REQ-MF              REQ-MF-001 - 009 9           Mutual fund parsing, CG
                                                   calculation

  REQ-STK             REQ-STK-001 -    7           Indian stock trading,
                      007                          dividends

  REQ-EPF             REQ-EPF-001 -    6           EPF passbook, contributions
                      006                          

  REQ-PPF             REQ-PPF-001 -    4           PPF tracking, 80C deduction
                      004                          

  REQ-NPS             REQ-NPS-001 -    5           NPS parsing, 80CCD
                      005                          deductions

  REQ-SAL             REQ-SAL-001 -    12          Salary, Form 16,
                      012                          perquisites

  REQ-TAX             REQ-TAX-001 -    9           Form 26AS, TDS/TCS
                      009                          reconciliation

  REQ-RNT             REQ-RNT-001 -    6           Rental income, Section 24
                      006                          

  REQ-SGB             REQ-SGB-001 -    5           SGB, RBI bonds tracking
                      005                          

  REQ-REIT            REQ-REIT-001 -   5           REIT/InvIT distributions
                      005                          

  REQ-RSU             REQ-RSU-001 -    6           RSU vest, sale, DRIP
                      006                          

  REQ-ESPP            REQ-ESPP-001 -   5           ESPP purchase, discount
                      005                          

  REQ-DTAA            REQ-DTAA-001 -   4           Foreign tax credit, Form 67
                      004                          

  REQ-FA              REQ-FA-001 - 004 4           Schedule FA reporting

  REQ-UNL             REQ-UNL-001 -    4           Unlisted share valuation
                      004                          

  REQ-RPT             REQ-RPT-001 -    8           Reports, exports, ITR JSON
                      008                          
  ------------------- ---------------- ----------- ---------------------------

2\. Core System Requirements

  ------------------ ---------------- --------------- ---------------------- --------------
  **Req ID**         **Name**         **Input**       **Processing**         **Output**

  **REQ-CORE-001**   Database Engine  None            Initialize SQLite      Encrypted
                                                      database with          database file
                                                      SQLCipher encryption   (.db)

  **REQ-CORE-002**   Chart of         Configuration   Create 18 asset class  Account
                     Accounts         file            accounts with GNUCash  hierarchy in
                                                      mapping                DB

  **REQ-CORE-003**   Journal Engine   Transaction     Double-entry           Journal
                                      data            validation, audit      entries with
                                                      trail creation         timestamps

  **REQ-CORE-004**   Multi-Currency   USD amounts,    Convert using SBI TT   INR equivalent
                     Support          date            Buying Rate            amounts

  **REQ-CORE-005**   Data Encryption  Sensitive       AES-256 encryption     Encrypted
                                      fields (PAN,                           field values
                                      Aadhaar)                               

  **REQ-CORE-006**   Audit Logging    User actions    Log all data           Audit trail
                                                      access/modifications   entries

  **REQ-CORE-007**   Session          User            Authenticate, manage   Session token,
                     Management       credentials     timeout                expiry
  ------------------ ---------------- --------------- ---------------------- --------------

3\. Bank Account Requirements

  ------------------ -------------------- -------------- ------------------ --------------
  **Req ID**         **Name**             **Input**      **Processing**     **Output**

  **REQ-BANK-001**   Bank Statement       Bank PDF/Excel Extract            Transaction
                     Parser               statement      transactions,      records in DB
                                                         categorize         
                                                         debits/credits     

  **REQ-BANK-002**   Interest Calculation Bank           Identify interest  Interest
                                          transactions   credits, apply     income ledger
                                                         80TTA/80TTB        entries

  **REQ-BANK-003**   Password-Protected   Encrypted PDF, Decrypt and parse  Transaction
                     PDF                  password                          data

  **REQ-BANK-004**   Multi-Account        Multiple bank  Merge by date,     Consolidated
                     Consolidation        statements     remove duplicates  transaction
                                                                            list
  ------------------ -------------------- -------------- ------------------ --------------

4\. Mutual Fund Requirements

  ---------------- ---------------- -------------- ------------------ --------------
  **Req ID**       **Name**         **Input**      **Processing**     **Output**

  **REQ-MF-001**   CAMS CAS Parser  CAMS PDF       Extract holdings,  MF transaction
                                    (password      transactions, NAV  records
                                    protected)                        

  **REQ-MF-002**   KARVY CAS Parser KARVY Excel    Extract            MF transaction
                                    file           scheme-wise        records
                                                   transactions       

  **REQ-MF-003**   Equity MF        Scheme name,   Classify as Equity Asset class
                   Classification   ISIN           (\>65% equity)     flag

  **REQ-MF-004**   Debt MF          Scheme name,   Classify as Debt   Asset class
                   Classification   ISIN           (\<65% equity)     flag

  **REQ-MF-005**   STCG Calculation MF             Calculate gain at  STCG amount,
                   (Equity)         transactions   20% rate           tax
                                    \<12 months                       

  **REQ-MF-006**   LTCG Calculation MF             Calculate gain at  LTCG amount,
                   (Equity)         transactions   12.5% (post Jul    tax
                                    \>12 months    2024)              

  **REQ-MF-007**   Debt MF Tax      Debt MF        Calculate gain at  Capital gain,
                   Calculation      transactions   slab rate (post    tax
                                                   Apr 2023)          

  **REQ-MF-008**   Grandfathering   Pre-2018       Apply FMV as on    Adjusted cost
                   (31-Jan-2018)    acquisitions   31-Jan-2018        basis

  **REQ-MF-009**   Capital Gain     All MF         Generate quarterly CG statement
                   Statement        transactions   breakdown          Excel
  ---------------- ---------------- -------------- ------------------ --------------

5\. Indian Stock Requirements

  ----------------- ------------- -------------- ------------------ --------------
  **Req ID**        **Name**      **Input**      **Processing**     **Output**

  **REQ-STK-001**   ICICI Direct  ICICI Direct   Extract trade-wise Stock
                    Parser        Excel/CSV      P&L                transaction
                                                                    records

  **REQ-STK-002**   Zerodha       Zerodha Tax    Extract equity,    Stock
                    Parser        P&L Excel      intraday,          transaction
                                                 dividends          records

  **REQ-STK-003**   Dividend      Dividend       Record dividend    Dividend
                    Tracking      entries from   income, TDS        ledger entries
                                  broker                            

  **REQ-STK-004**   STCG          Stock sales    Calculate gain at  STCG amount
                    Calculation   \<12 months    20% rate           
                    (Stocks)                                        

  **REQ-STK-005**   LTCG          Stock sales    Calculate gain at  LTCG amount
                    Calculation   \>12 months    12.5%              
                    (Stocks)                                        

  **REQ-STK-006**   STT Tracking  Trade data     Extract STT paid   STT ledger
                                                 from statements    entries

  **REQ-STK-007**   Holdings      All stock      Calculate current  Holdings
                    Report        transactions   holdings, cost     statement
                                                 basis              
  ----------------- ------------- -------------- ------------------ --------------

6\. EPF Requirements

  ----------------- -------------- --------------- ------------------ --------------
  **Req ID**        **Name**       **Input**       **Processing**     **Output**

  **REQ-EPF-001**   EPF Passbook   EPFO PDF        Extract            EPF
                    Parser         passbook        contributions,     transaction
                                                   interest, TDS      records

  **REQ-EPF-002**   Employee       Monthly         Record 12% of      EE
                    Contribution   passbook entry  Basic to EE        contribution
                                                   account            ledger

  **REQ-EPF-003**   Employer       Monthly         Record ER          ER
                    Contribution   passbook entry  contribution + EPS contribution
                                                                      ledger

  **REQ-EPF-004**   VPF            VPF entries in  Separate VPF from  VPF ledger
                    Contribution   passbook        statutory PF       entries

  **REQ-EPF-005**   Interest       Yearly interest Record interest,   Interest
                    Calculation    entry           handle TDS on      income, TDS
                                                   \>2.5L             

  **REQ-EPF-006**   80C Deduction  EE+VPF          Sum eligible       80C eligible
                                   contributions   amount up to 1.5L  amount
  ----------------- -------------- --------------- ------------------ --------------

7\. PPF Requirements

  ----------------- ------------- -------------- ------------------ --------------
  **Req ID**        **Name**      **Input**      **Processing**     **Output**

  **REQ-PPF-001**   PPF Statement Bank PPF       Extract deposits,  PPF
                    Parser        statement      interest           transaction
                                  Excel                             records

  **REQ-PPF-002**   Interest      Monthly        Calculate at govt  Interest
                    Calculation   balances       rate (currently    credited
                                                 7.1%)              

  **REQ-PPF-003**   80C Deduction Annual         Sum deposits up to 80C eligible
                                  deposits       1.5L limit         amount

  **REQ-PPF-004**   Maturity      Account open   Calculate 15-year  Maturity date,
                    Tracking      date           maturity           status
  ----------------- ------------- -------------- ------------------ --------------

8\. NPS Requirements

  ----------------- -------------- --------------- ------------------ --------------
  **Req ID**        **Name**       **Input**       **Processing**     **Output**

  **REQ-NPS-001**   NPS Statement  NPS CSV/PDF     Extract            NPS
                    Parser         statement       contributions,     transaction
                                                   NAV, units         records

  **REQ-NPS-002**   Employee       Monthly         Record Tier I/II   NPS
                    Contribution   contributions   contributions      contribution
                                                                      ledger

  **REQ-NPS-003**   Employer       NPS ER          Track 80CCD(2)     ER
                    Contribution   contribution    eligible (10% of   contribution
                                                   Basic)             ledger

  **REQ-NPS-004**   80CCD(1B)      Additional NPS  Track up to 50K    80CCD(1B)
                    Deduction      contribution    additional         amount
                                                   deduction          

  **REQ-NPS-005**   NAV Tracking   Statement NAV   Store scheme-wise  NAV records
                                   data            NAV history        
  ----------------- -------------- --------------- ------------------ --------------

9\. Salary & Form 16 Requirements

  ----------------- -------------- -------------- ------------------- ----------------
  **Req ID**        **Name**       **Input**      **Processing**      **Output**

  **REQ-SAL-001**   Payslip Parser Monthly        Extract all         Salary component
                                   payslip PDF    earning/deduction   records
                                                  components          

  **REQ-SAL-002**   Basic Salary   Payslip Basic  Record monthly      Basic salary
                    Tracking       field          Basic salary        ledger

  **REQ-SAL-003**   HRA Exemption  HRA, Basic,    Calculate Sec       HRA exempt
                                   Rent, City     10(13A) exemption   amount

  **REQ-SAL-004**   LTA Exemption  LTA claimed,   Calculate Sec 10(5) LTA exempt
                                   bills          exemption           amount

  **REQ-SAL-005**   RSU Tax Credit Negative RSU   Handle perquisite   RSU tax credit
                                   tax deduction  tax reversal        ledger

  **REQ-SAL-006**   ESPP Deduction ESPP payroll   Track after-tax     ESPP deduction
                                   deduction      ESPP investment     ledger

  **REQ-SAL-007**   TCS on ESPP    TCS deducted   Track TCS claimable TCS credit
                                   for LRS        as credit           ledger

  **REQ-SAL-008**   Professional   PT deduction   Record PT for       PT ledger entry
                    Tax                           salary deduction    

  **REQ-SAL-009**   Form 16 Part A Form 16 Part A Extract quarterly   TDS records by
                    Parser         PDF/ZIP        TDS, challans       quarter

  **REQ-SAL-010**   Form 16 Part B Form 16 Part B Extract salary      Annual salary
                    Parser         PDF/ZIP        breakup, deductions summary

  **REQ-SAL-011**   Form 12BA      Form 12BA      Extract perquisite  Perquisite
                    Parser         PDF/ZIP        details             records

  **REQ-SAL-012**   Salary         Employer tax   Extract detailed    Component-wise
                    Annexure       annexure PDF   salary components   salary
                    Parser                                            
  ----------------- -------------- -------------- ------------------- ----------------

10\. Income Tax Statement Requirements

  ----------------- ---------------- -------------- ------------------ ----------------
  **Req ID**        **Name**         **Input**      **Processing**     **Output**

  **REQ-TAX-001**   Form 26AS Parser Form 26AS      Extract TDS, TCS,  26AS transaction
                                     PDF/ZIP        advance tax,       records
                                                    refunds            

  **REQ-TAX-002**   TDS Section 192  26AS Part I    Extract employer   Salary TDS
                    (Salary)                        TDS details        records

  **REQ-TAX-003**   TDS Section 194  26AS Part I    Extract dividend   Dividend TDS
                    (Dividend)                      TDS                records

  **REQ-TAX-004**   TDS Section 194A 26AS Part I    Extract FD/RD      Interest TDS
                    (Interest)                      interest TDS       records

  **REQ-TAX-005**   TCS Section      26AS Part VI   Extract TCS on     TCS credit
                    206CQ (LRS)                     foreign remittance records

  **REQ-TAX-006**   Advance Tax      Challan 280    Extract CIN,       Advance tax
                    Parser           PDF            amount, date       records

  **REQ-TAX-007**   TDS              26AS + Form    Match TDS across   Reconciliation
                    Reconciliation   16 + Salary    sources            report

  **REQ-TAX-008**   TCS              26AS + Salary  Match TCS across   TCS
                    Reconciliation   annexure       sources            reconciliation
                                                                       report

  **REQ-TAX-009**   Status Code      26AS booking   Flag F/U/M/P/O/Z   Status alerts
                    Handling         status         status             
  ----------------- ---------------- -------------- ------------------ ----------------

11\. Rental Income Requirements

  ----------------- -------------- -------------- ------------------ --------------
  **Req ID**        **Name**       **Input**      **Processing**     **Output**

  **REQ-RNT-001**   Property       Property       Store address,     Property
                    Registration   details        tenant, rent       master record
                                                  amount             

  **REQ-RNT-002**   Rental Income  Monthly rent   Record gross rent  Rental income
                    Tracking       receipts       received           ledger

  **REQ-RNT-003**   Standard       Annual rental  Apply 30% standard Net rental
                    Deduction      income         deduction          income

  **REQ-RNT-004**   Section 24     Home loan      Deduct interest up Interest
                    Interest       interest       to 2L limit        deduction

  **REQ-RNT-005**   Municipal Tax  Property tax   Deduct from gross  Tax deduction
                                   paid           rent               ledger

  **REQ-RNT-006**   Loss from      Negative net   Calculate loss,    Loss amount
                    House Property income         max 2L set-off     
  ----------------- -------------- -------------- ------------------ --------------

12\. SGB & RBI Bond Requirements

  ----------------- ------------- -------------- ------------------ --------------
  **Req ID**        **Name**      **Input**      **Processing**     **Output**

  **REQ-SGB-001**   SGB Holdings  NSDL CAS/Demat Extract SGB        SGB holding
                    Parser        statement      series, units, FMV records

  **REQ-SGB-002**   SGB Interest  Semi-annual    Record 2.5%        Interest
                    Tracking      interest       interest income    ledger entry
                                  credit                            

  **REQ-SGB-003**   SGB Maturity  8-year         Mark CG as exempt  Exempt CG
                    CG            maturity event                    record

  **REQ-SGB-004**   RBI Bond      Bank statement Extract floating   RBI bond
                    Parser        interest       rate interest      interest
                                                                    records

  **REQ-SGB-005**   RBI Bond Rate NSC rate +     Calculate current  Rate history
                    Tracking      35bps          rate               
  ----------------- ------------- -------------- ------------------ --------------

13\. REIT/InvIT Requirements

  ------------------ -------------- -------------- ------------------ --------------
  **Req ID**         **Name**       **Input**      **Processing**     **Output**

  **REQ-REIT-001**   REIT Holdings  Demat          Extract REIT       REIT holding
                     Parser         statement      units, cost basis  records

  **REQ-REIT-002**   Distribution   REIT           Split: dividend,   Distribution
                     Breakdown      distribution   interest, other    components
                                    statement                         

  **REQ-REIT-003**   Dividend       Dividend       Record as exempt   Exempt
                     Income         portion        (from SPV)         dividend
                                                                      ledger

  **REQ-REIT-004**   Interest       Interest       Record at slab     Interest
                     Income         portion        rate, track TDS    ledger, TDS

  **REQ-REIT-005**   Capital        Capital        Reduce cost basis  Adjusted cost
                     Reduction      reduction                         basis
                                    amount                            
  ------------------ -------------- -------------- ------------------ --------------

14\. Foreign Assets - RSU Requirements

  ----------------- ------------- ----------------- ------------------ --------------
  **Req ID**        **Name**      **Input**         **Processing**     **Output**

  **REQ-RSU-001**   RSU Vest      E\*TRADE/Morgan   Extract vest date, RSU vest
                    Parser        Stanley statement FMV, shares        records

  **REQ-RSU-002**   Perquisite    Vest FMV, TT rate Calculate          Perquisite
                    Calculation                     perquisite value   amount
                                                    in INR             

  **REQ-RSU-003**   RSU-Salary    Vest events +     Match vest with    Correlation
                    Correlation   payslip credits   tax credit         report

  **REQ-RSU-004**   Cost Basis    Vest FMV per      Store for future   Cost basis
                    Tracking      share             sale CG calc       records

  **REQ-RSU-005**   RSU Sale CG   Sale proceeds,    Calculate STCG     Capital gain
                                  cost basis        (\<24mo) or LTCG   

  **REQ-RSU-006**   DRIP          Dividend          Record dividend +  DRIP ledger
                    Processing    reinvestment      new share purchase entries
  ----------------- ------------- ----------------- ------------------ --------------

15\. Foreign Assets - ESPP Requirements

  ------------------ ------------- -------------- ------------------ --------------
  **Req ID**         **Name**      **Input**      **Processing**     **Output**

  **REQ-ESPP-001**   ESPP Purchase E\*TRADE ESPP  Extract purchase   ESPP purchase
                     Parser        statement      date, discount,    records
                                                  shares             

  **REQ-ESPP-002**   Discount      Market price,  Calculate 15%      Perquisite
                     Perquisite    purchase price discount as        amount
                                                  perquisite         

  **REQ-ESPP-003**   Cost Basis    Purchase price Store for future   Cost basis
                     (ESPP)        per share      sale CG calc       records

  **REQ-ESPP-004**   ESPP Sale CG  Sale proceeds, Calculate STCG     Capital gain
                                   cost basis     (\<24mo) or LTCG   

  **REQ-ESPP-005**   TCS on LRS    ESPP           Calculate 20% TCS  TCS credit
                                   remittance     under 206CQ        
                                   amount                            
  ------------------ ------------- -------------- ------------------ --------------

16\. DTAA & Foreign Tax Credit Requirements

  ------------------ ------------- -------------- ------------------ --------------
  **Req ID**         **Name**      **Input**      **Processing**     **Output**

  **REQ-DTAA-001**   US            Form 1042-S    Extract            US tax
                     Withholding                  withholding amount withheld
                     Parser                                          records

  **REQ-DTAA-002**   DTAA Credit   US tax, India  Calculate lower of DTAA credit
                     Calculation   tax liability  US tax or India    amount
                                                  tax                

  **REQ-DTAA-003**   Form 67       Foreign        Generate Form 67   Form 67 data
                     Generation    income, tax    for DTAA claim     
                                   paid                              

  **REQ-DTAA-004**   Tax Credit    DTAA credits   Track foreign tax  Tax credit
                     Ledger                       credits            ledger
  ------------------ ------------- -------------- ------------------ --------------

17\. Schedule FA (Foreign Assets) Requirements

  ---------------- ------------- -------------- ------------------ --------------
  **Req ID**       **Name**      **Input**      **Processing**     **Output**

  **REQ-FA-001**   Foreign Bank  US brokerage   Report peak        FA Schedule A1
                   Account       account        balance, account   
                                                details            

  **REQ-FA-002**   Foreign       US stock       Report stock       FA Schedule A2
                   Equity        holdings       details, cost      
                   Holding                      basis              

  **REQ-FA-003**   Foreign       Dividend,      Report income from FA income
                   Income        interest, CG   foreign assets     section

  **REQ-FA-004**   Schedule FA   All foreign    Generate ITR       Schedule FA
                   Generation    asset data     Schedule FA JSON   JSON
  ---------------- ------------- -------------- ------------------ --------------

18\. Unlisted Shares Requirements

  ----------------- ------------- -------------- ------------------ --------------
  **Req ID**        **Name**      **Input**      **Processing**     **Output**

  **REQ-UNL-001**   Unlisted      Acquisition    Store company,     Unlisted share
                    Share Tracker details        shares, cost       records

  **REQ-UNL-002**   Fair Value    Company        Calculate FMV per  FMV per share
                    Calculation   financials     CBDT rules         

  **REQ-UNL-003**   LTCG          Sale after 24  Calculate LTCG at  LTCG amount
                    Calculation   months         12.5%              
                    (24mo)                                          

  **REQ-UNL-004**   Schedule UA   Unlisted share Generate ITR       Schedule UA
                    Generation    data           Schedule UA        JSON
  ----------------- ------------- -------------- ------------------ --------------

19\. Reporting & Export Requirements

  ----------------- ------------- -------------- ------------------ ---------------
  **Req ID**        **Name**      **Input**      **Processing**     **Output**

  **REQ-RPT-001**   Net Worth     All asset data Calculate total    Net worth
                    Report                       assets,            statement
                                                 liabilities        

  **REQ-RPT-002**   Tax           All income,    Compute tax under  Tax computation
                    Computation   deductions     Old/New regime     sheet

  **REQ-RPT-003**   GNUCash QIF   Journal        Convert to QIF     QIF file
                    Export        entries        format             

  **REQ-RPT-004**   GNUCash CSV   Journal        Convert to GNUCash CSV file
                    Export        entries        CSV format         

  **REQ-RPT-005**   Capital Gains All CG         Generate ITR CG    CG report Excel
                    Report        transactions   schedules          

  **REQ-RPT-006**   Advance Tax   YTD income,    Calculate          Advance tax
                    Calculator    projections    quarterly          schedule
                                                 installments       

  **REQ-RPT-007**   ITR-2 JSON    All tax data   Generate ITR-2     ITR-2 JSON file
                    Export                       JSON per schema    

  **REQ-RPT-008**   Asset         Holdings by    Generate pie chart Chart
                    Allocation    asset class    data               visualization
                    Chart                                           
  ----------------- ------------- -------------- ------------------ ---------------

20\. Critical Technical Decisions

20.1 RSU Tax Credit Handling

RSU perquisite tax appears as **NEGATIVE deduction (credit)** in months
when RSUs vest. This reverses excess TDS withheld earlier. The system
must:

1.  Track negative RSU tax entries in payslips

2.  Correlate with E\*TRADE vest events from Phase 2

3.  Reconcile total perquisite tax across payslips and Form 16

20.2 Currency Conversion

All USD to INR conversions must use **SBI TT Buying Rate** as per RBI
guidelines. The rate applicable is the rate on the date of:

4.  RSU vest (for perquisite calculation)

5.  Stock sale (for capital gains calculation)

6.  Dividend receipt (for income reporting)

20.3 LTCG Holding Period

  ----------------------- ------------------- ---------------------------
  **Asset Type**          **LTCG Threshold**  **Tax Rate**

  Indian Listed Equity/MF 12 months           12.5% (post Jul 2024)

  Foreign Stocks          24 months           12.5% with DTAA credit
  (RSU/ESPP)                                  

  Unlisted Shares         24 months           12.5%

  Debt MF (post Apr 2023) N/A                 Slab rate (no LTCG benefit)
  ----------------------- ------------------- ---------------------------

*\-\-- End of Requirements Document v6.0 \-\--*
