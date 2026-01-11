Personal Financial Accounting System

**Test Cases & Coverage Matrix**

**Version 6.0**

January 2026

1\. Test Summary

**Total Test Cases:** 110 **\| Requirements Covered:** 110 **\|
Coverage:** 100%

  --------------------------- ----------- ----------- -------------------
  **Category**                **Tests**   **P1**      **Phase**

  Core System                 7           5           Phase 1

  Bank Account                4           3           Phase 1

  Mutual Funds                9           8           Phase 1

  Indian Stocks               7           5           Phase 1

  EPF                         6           5           Phase 1

  PPF                         4           3           Phase 1

  NPS                         5           4           Phase 1

  Salary & Form 16            12          9           Phase 1

  Income Tax Statements       9           8           Phase 1

  Rental Income               6           4           Phase 1

  SGB & RBI Bonds             5           4           Phase 1

  REIT/InvIT                  5           4           Phase 1

  RSU (Phase 2)               6           5           Phase 2

  ESPP (Phase 2)              5           5           Phase 2

  DTAA (Phase 2)              4           3           Phase 2

  Schedule FA (Phase 2)       4           4           Phase 2

  Unlisted Shares (Phase 2)   4           3           Phase 2

  Reports & Export            8           4           Phase 1
  --------------------------- ----------- ----------- -------------------

2\. Core System Tests

  ----------------- -------------- -------------- --------------- ---------------- -------
  **Test ID**       **Req ID**     **Name**       **Input**       **Expected**     **P**

  **TC-CORE-001**   REQ-CORE-001   DB Encryption  Init command    SQLCipher        P1
                                   Init                           encrypted DB     
                                                                  created          

  **TC-CORE-002**   REQ-CORE-002   Chart of       Config file     18 asset class   P1
                                   Accounts Setup                 accounts created 

  **TC-CORE-003**   REQ-CORE-003   Journal        Unbalanced      Error: Dr != Cr  P1
                                   Balance        entry                            
                                   Validation                                      

  **TC-CORE-004**   REQ-CORE-004   USD to INR     USD 100,        INR at SBI TT    P1
                                   Conversion     2024-06-15      rate             

  **TC-CORE-005**   REQ-CORE-005   PAN Encryption AAPPS0793R      Encrypted value  P1
                                                                  stored           

  **TC-CORE-006**   REQ-CORE-006   Audit Log      Any data change Audit record     P2
                                   Entry                          created          

  **TC-CORE-007**   REQ-CORE-007   Session        15 min idle     Session expired  P2
                                   Timeout                                         
  ----------------- -------------- -------------- --------------- ---------------- -------

3\. Bank Account Tests

  ----------------- -------------- -------------- --------------- ---------------- -------
  **Test ID**       **Req ID**     **Name**       **Input**       **Expected**     **P**

  **TC-BANK-001**   REQ-BANK-001   ICICI          ICICI PDF stmt  Transactions     P1
                                   Statement                      extracted        
                                   Parse                                           

  **TC-BANK-002**   REQ-BANK-002   Interest 80TTA Bank interest   80TTA deduction  P1
                                                  ₹12,000         applied          

  **TC-BANK-003**   REQ-BANK-003   Password PDF   Encrypted PDF   Decrypted and    P1
                                                                  parsed           

  **TC-BANK-004**   REQ-BANK-004   Multi-Bank     3 bank          Consolidated by  P2
                                   Merge          statements      date             
  ----------------- -------------- -------------- --------------- ---------------- -------

4\. Mutual Funds Tests

  --------------- ------------ -------------- --------------- ---------------- -------
  **Test ID**     **Req ID**   **Name**       **Input**       **Expected**     **P**

  **TC-MF-001**   REQ-MF-001   CAMS CAS Parse CAMS PDF        All folios       P1
                                                              extracted        

  **TC-MF-002**   REQ-MF-002   KARVY Parse    KARVY Excel     Transactions     P1
                                                              imported         

  **TC-MF-003**   REQ-MF-003   Equity MF Flag SBI Bluechip    asset_class =    P1
                                              Fund            EQUITY           

  **TC-MF-004**   REQ-MF-004   Debt MF Flag   HDFC Short Term asset_class =    P1
                                                              DEBT             

  **TC-MF-005**   REQ-MF-005   Equity STCG    Sale \<12       20% tax          P1
                               Calc           months          calculated       

  **TC-MF-006**   REQ-MF-006   Equity LTCG    Sale \>12       12.5% tax        P1
                               Calc           months          calculated       

  **TC-MF-007**   REQ-MF-007   Debt MF Slab   Debt MF sale    Slab rate        P1
                               Tax                            applied          

  **TC-MF-008**   REQ-MF-008   Grandfather    Pre-2018        FMV as cost      P1
                               31-Jan-18      purchase        basis            

  **TC-MF-009**   REQ-MF-009   CG Statement   FY transactions Quarterly Excel  P2
                               Gen                            report           
  --------------- ------------ -------------- --------------- ---------------- -------

5\. Indian Stocks Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-STK-001**   REQ-STK-001   ICICI Direct   ICICI CSV       Trades imported  P1
                                 Parse                                           

  **TC-STK-002**   REQ-STK-002   Zerodha Parse  Zerodha Tax P&L All sheets       P1
                                                                parsed           

  **TC-STK-003**   REQ-STK-003   Dividend       Dividend entry  Income + TDS     P1
                                 Record                         recorded         

  **TC-STK-004**   REQ-STK-004   Stock STCG     Sale \<12       20% STCG tax     P1
                                                months                           

  **TC-STK-005**   REQ-STK-005   Stock LTCG     Sale \>12       12.5% LTCG tax   P1
                                                months                           

  **TC-STK-006**   REQ-STK-006   STT Tracking   Trade with STT  STT ledger entry P2

  **TC-STK-007**   REQ-STK-007   Holdings       All             Current holdings P2
                                 Report         transactions    list             
  ---------------- ------------- -------------- --------------- ---------------- -------

6\. EPF Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-EPF-001**   REQ-EPF-001   EPF Passbook   EPFO PDF        Contributions    P1
                                 Parse                          extracted        

  **TC-EPF-002**   REQ-EPF-002   EE             Monthly entry   12% of Basic     P1
                                 Contribution                   recorded         

  **TC-EPF-003**   REQ-EPF-003   ER             Monthly entry   ER + EPS         P1
                                 Contribution                   recorded         

  **TC-EPF-004**   REQ-EPF-004   VPF Separation VPF entries     VPF split from   P2
                                                                EE PF            

  **TC-EPF-005**   REQ-EPF-005   Interest + TDS Interest \>2.5L TDS deducted     P1

  **TC-EPF-006**   REQ-EPF-006   80C Eligible   EE+VPF total    80C amount       P1
                                                                calculated       
  ---------------- ------------- -------------- --------------- ---------------- -------

7\. PPF Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-PPF-001**   REQ-PPF-001   PPF Statement  Bank PPF Excel  Transactions     P1
                                 Parse                          imported         

  **TC-PPF-002**   REQ-PPF-002   Interest Calc  Monthly         7.1% interest    P1
                                                balances        applied          

  **TC-PPF-003**   REQ-PPF-003   80C Deduction  Annual deposits Up to 1.5L       P1
                                                                deduction        

  **TC-PPF-004**   REQ-PPF-004   Maturity Date  Open date       15-year maturity P2
                                                                set              
  ---------------- ------------- -------------- --------------- ---------------- -------

8\. NPS Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-NPS-001**   REQ-NPS-001   NPS CSV Parse  NPS statement   Contributions    P1
                                                                extracted        

  **TC-NPS-002**   REQ-NPS-002   EE NPS         Monthly entry   Tier I/II        P1
                                 Contribution                   recorded         

  **TC-NPS-003**   REQ-NPS-003   ER 80CCD(2)    ER NPS contrib  10% of Basic     P1
                                                                limit            

  **TC-NPS-004**   REQ-NPS-004   80CCD(1B)      Additional 50K  Extra deduction  P1
                                 Extra                          tracked          

  **TC-NPS-005**   REQ-NPS-005   NAV History    Statement NAV   NAV history      P2
                                                                stored           
  ---------------- ------------- -------------- --------------- ---------------- -------

9\. Salary & Form 16 Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-SAL-001**   REQ-SAL-001   Payslip Parse  Monthly PDF     All components   P1
                                                payslip         extracted        

  **TC-SAL-002**   REQ-SAL-002   Basic Salary   Payslip Basic   Basic ledger     P1
                                                                entry            

  **TC-SAL-003**   REQ-SAL-003   HRA Exemption  HRA, Basic,     Sec 10(13A)      P1
                                 Calc           Rent            calculated       

  **TC-SAL-004**   REQ-SAL-004   LTA Exemption  LTA claimed     Sec 10(5)        P2
                                                                applied          

  **TC-SAL-005**   REQ-SAL-005   RSU Tax Credit Negative RSU    Credit ledger    P1
                                                tax             entry            

  **TC-SAL-006**   REQ-SAL-006   ESPP Deduction ESPP payroll    ESPP investment  P1
                                                ded             tracked          

  **TC-SAL-007**   REQ-SAL-007   TCS on ESPP    TCS deducted    TCS credit       P1
                                                                recorded         

  **TC-SAL-008**   REQ-SAL-008   Professional   PT deduction    PT ledger entry  P2
                                 Tax                                             

  **TC-SAL-009**   REQ-SAL-009   Form 16A Parse Form 16 Part A  Quarterly TDS    P1
                                                ZIP             extracted        

  **TC-SAL-010**   REQ-SAL-010   Form 16B Parse Form 16 Part B  Salary breakup   P1
                                                ZIP             imported         

  **TC-SAL-011**   REQ-SAL-011   Form 12BA      Form 12BA PDF   Perquisites      P1
                                 Parse                          extracted        

  **TC-SAL-012**   REQ-SAL-012   Salary         Employer        Component-wise   P2
                                 Annexure       annexure        data             
  ---------------- ------------- -------------- --------------- ---------------- -------

10\. Income Tax Statements Tests

  ---------------- ------------- ---------------- --------------- ----------------- -------
  **Test ID**      **Req ID**    **Name**         **Input**       **Expected**      **P**

  **TC-TAX-001**   REQ-TAX-001   Form 26AS Parse  26AS ZIP/PDF    TDS/TCS/Advance   P1
                                                                  extracted         

  **TC-TAX-002**   REQ-TAX-002   Salary TDS (192) 26AS Part I     Employer TDS      P1
                                                                  recorded          

  **TC-TAX-003**   REQ-TAX-003   Dividend TDS     26AS Sec 194    Dividend TDS      P1
                                 (194)                            extracted         

  **TC-TAX-004**   REQ-TAX-004   Interest TDS     26AS Sec 194A   FD interest TDS   P1
                                 (194A)                                             

  **TC-TAX-005**   REQ-TAX-005   LRS TCS (206CQ)  26AS Part VI    ESPP TCS          P1
                                                                  extracted         

  **TC-TAX-006**   REQ-TAX-006   Challan Parse    Challan 280 PDF CIN, amount       P1
                                                                  extracted         

  **TC-TAX-007**   REQ-TAX-007   TDS              26AS + Form 16  Variance report   P1
                                 Reconciliation                                     

  **TC-TAX-008**   REQ-TAX-008   TCS              26AS + Annexure TCS match report  P1
                                 Reconciliation                                     

  **TC-TAX-009**   REQ-TAX-009   Status Code      Status = U      Alert for         P2
                                 Alert                            unmatched         
  ---------------- ------------- ---------------- --------------- ----------------- -------

11\. Rental Income Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-RNT-001**   REQ-RNT-001   Property       Property        Property master  P1
                                 Register       details         created          

  **TC-RNT-002**   REQ-RNT-002   Rental Income  Monthly rent    Income ledger    P1
                                 Entry                          entry            

  **TC-RNT-003**   REQ-RNT-003   30% Std        Annual rent     30% deduction    P1
                                 Deduction                      applied          

  **TC-RNT-004**   REQ-RNT-004   Sec 24         Home loan       Up to 2L         P1
                                 Interest       interest        deduction        

  **TC-RNT-005**   REQ-RNT-005   Municipal Tax  Property tax    Deducted from    P2
                                                paid            gross            

  **TC-RNT-006**   REQ-RNT-006   HP Loss        Negative net    Loss up to 2L    P2
                                                income          set-off          
  ---------------- ------------- -------------- --------------- ---------------- -------

12\. SGB & RBI Bonds Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-SGB-001**   REQ-SGB-001   SGB Holdings   NSDL CAS        SGB series       P1
                                 Parse                          extracted        

  **TC-SGB-002**   REQ-SGB-002   SGB Interest   Semi-annual     2.5% interest    P1
                                                credit          recorded         

  **TC-SGB-003**   REQ-SGB-003   SGB Maturity   8-year maturity CG marked exempt P1
                                 Exempt                                          

  **TC-SGB-004**   REQ-SGB-004   RBI Bond       Bank statement  Interest         P1
                                 Interest                       extracted        

  **TC-SGB-005**   REQ-SGB-005   RBI Rate Track NSC rate change New rate         P2
                                                                calculated       
  ---------------- ------------- -------------- --------------- ---------------- -------

13\. REIT/InvIT Tests

  ----------------- -------------- -------------- --------------- ---------------- -------
  **Test ID**       **Req ID**     **Name**       **Input**       **Expected**     **P**

  **TC-REIT-001**   REQ-REIT-001   REIT Holdings  Demat statement Units imported   P1

  **TC-REIT-002**   REQ-REIT-002   Distribution   Dist statement  Div/Int/Other    P1
                                   Split                          split            

  **TC-REIT-003**   REQ-REIT-003   REIT Dividend  Dividend        Exempt income    P1
                                                  portion                          

  **TC-REIT-004**   REQ-REIT-004   REIT Interest  Interest        Taxable + TDS    P1
                                                  portion                          

  **TC-REIT-005**   REQ-REIT-005   Capital        Cap reduction   Cost basis       P2
                                   Reduction      amt             reduced          
  ----------------- -------------- -------------- --------------- ---------------- -------

14\. RSU (Phase 2) Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-RSU-001**   REQ-RSU-001   RSU Vest Parse Morgan Stanley  Vest details     P1
                                                stmt            extracted        

  **TC-RSU-002**   REQ-RSU-002   Perquisite INR FMV USD, TT     Perquisite in    P1
                                 Calc           rate            INR              

  **TC-RSU-003**   REQ-RSU-003   RSU-Salary     Vest + payslip  Correlation      P1
                                 Match                          report           

  **TC-RSU-004**   REQ-RSU-004   RSU Cost Basis Vest FMV        Cost basis       P1
                                                                stored           

  **TC-RSU-005**   REQ-RSU-005   RSU Sale LTCG  Sale \>24       LTCG at 12.5%    P1
                                                months                           

  **TC-RSU-006**   REQ-RSU-006   DRIP           DRIP            Dividend +       P2
                                 Processing     transaction     purchase         
  ---------------- ------------- -------------- --------------- ---------------- -------

15\. ESPP (Phase 2) Tests

  ----------------- -------------- -------------- --------------- ---------------- -------
  **Test ID**       **Req ID**     **Name**       **Input**       **Expected**     **P**

  **TC-ESPP-001**   REQ-ESPP-001   ESPP Purchase  E\*TRADE stmt   Purchase         P1
                                   Parse                          extracted        

  **TC-ESPP-002**   REQ-ESPP-002   Discount       Market vs       15% discount     P1
                                   Perquisite     purchase        taxed            

  **TC-ESPP-003**   REQ-ESPP-003   ESPP Cost      Purchase price  Cost basis       P1
                                   Basis                          stored           

  **TC-ESPP-004**   REQ-ESPP-004   ESPP Sale CG   Sale            CG calculated    P1
                                                  transaction                      

  **TC-ESPP-005**   REQ-ESPP-005   TCS on LRS     ESPP remittance 20% TCS          P1
                                                                  calculated       
  ----------------- -------------- -------------- --------------- ---------------- -------

16\. DTAA (Phase 2) Tests

  ----------------- -------------- -------------- --------------- ---------------- -------
  **Test ID**       **Req ID**     **Name**       **Input**       **Expected**     **P**

  **TC-DTAA-001**   REQ-DTAA-001   US Withholding Form 1042-S     US tax extracted P1
                                   Parse                                           

  **TC-DTAA-002**   REQ-DTAA-002   DTAA Credit    US tax, India   Lower amount     P1
                                   Calc           tax             credit           

  **TC-DTAA-003**   REQ-DTAA-003   Form 67        Foreign income  Form 67 JSON     P1
                                   Generation     data                             

  **TC-DTAA-004**   REQ-DTAA-004   Tax Credit     DTAA credits    Credit ledger    P2
                                   Ledger                         entries          
  ----------------- -------------- -------------- --------------- ---------------- -------

17\. Schedule FA (Phase 2) Tests

  --------------- ------------ -------------- --------------- ---------------- -------
  **Test ID**     **Req ID**   **Name**       **Input**       **Expected**     **P**

  **TC-FA-001**   REQ-FA-001   Foreign Bank   US brokerage    Peak balance     P1
                               A/c                            reported         

  **TC-FA-002**   REQ-FA-002   Foreign Equity US stock        Holdings         P1
                                              holdings        reported         

  **TC-FA-003**   REQ-FA-003   Foreign Income Div, Int, CG    Income reported  P1

  **TC-FA-004**   REQ-FA-004   Schedule FA    All foreign     ITR JSON         P1
                               JSON           data            generated        
  --------------- ------------ -------------- --------------- ---------------- -------

18\. Unlisted Shares (Phase 2) Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-UNL-001**   REQ-UNL-001   Unlisted       Acquisition     Holdings         P1
                                 Register       data            recorded         

  **TC-UNL-002**   REQ-UNL-002   FMV            Company         FMV per CBDT     P1
                                 Calculation    financials                       

  **TC-UNL-003**   REQ-UNL-003   Unlisted LTCG  Sale \>24       12.5% tax        P1
                                                months                           

  **TC-UNL-004**   REQ-UNL-004   Schedule UA    Unlisted data   ITR JSON         P2
                                 JSON                           generated        
  ---------------- ------------- -------------- --------------- ---------------- -------

19\. Reports & Export Tests

  ---------------- ------------- -------------- --------------- ---------------- -------
  **Test ID**      **Req ID**    **Name**       **Input**       **Expected**     **P**

  **TC-RPT-001**   REQ-RPT-001   Net Worth      All assets      NW statement     P1
                                 Report                                          

  **TC-RPT-002**   REQ-RPT-002   Tax            Income,         Old/New regime   P1
                                 Computation    deductions      calc             

  **TC-RPT-003**   REQ-RPT-003   QIF Export     Journal entries Valid QIF file   P2

  **TC-RPT-004**   REQ-RPT-004   GNUCash CSV    Journal entries GNUCash CSV file P2

  **TC-RPT-005**   REQ-RPT-005   CG Report      CG transactions CG Excel report  P1

  **TC-RPT-006**   REQ-RPT-006   Advance Tax    YTD income      Quarterly        P2
                                 Calc                           amounts          

  **TC-RPT-007**   REQ-RPT-007   ITR-2 JSON     All tax data    Valid ITR JSON   P1
                                 Export                                          

  **TC-RPT-008**   REQ-RPT-008   Asset          Holdings        Pie chart data   P3
                                 Allocation                                      
  ---------------- ------------- -------------- --------------- ---------------- -------

Requirement-to-Test Coverage Matrix

Every requirement has at least one test case. Green indicates covered.

**Core System:** REQ-CORE-001→TC-CORE-001, REQ-CORE-002→TC-CORE-002,
REQ-CORE-003→TC-CORE-003, REQ-CORE-004→TC-CORE-004,
REQ-CORE-005→TC-CORE-005, REQ-CORE-006→TC-CORE-006,
REQ-CORE-007→TC-CORE-007

**Bank Account:** REQ-BANK-001→TC-BANK-001, REQ-BANK-002→TC-BANK-002,
REQ-BANK-003→TC-BANK-003, REQ-BANK-004→TC-BANK-004

**Mutual Funds:** REQ-MF-001→TC-MF-001, REQ-MF-002→TC-MF-002,
REQ-MF-003→TC-MF-003, REQ-MF-004→TC-MF-004, REQ-MF-005→TC-MF-005,
REQ-MF-006→TC-MF-006, REQ-MF-007→TC-MF-007, REQ-MF-008→TC-MF-008,
REQ-MF-009→TC-MF-009

**Indian Stocks:** REQ-STK-001→TC-STK-001, REQ-STK-002→TC-STK-002,
REQ-STK-003→TC-STK-003, REQ-STK-004→TC-STK-004, REQ-STK-005→TC-STK-005,
REQ-STK-006→TC-STK-006, REQ-STK-007→TC-STK-007

**EPF:** REQ-EPF-001→TC-EPF-001, REQ-EPF-002→TC-EPF-002,
REQ-EPF-003→TC-EPF-003, REQ-EPF-004→TC-EPF-004, REQ-EPF-005→TC-EPF-005,
REQ-EPF-006→TC-EPF-006

**PPF:** REQ-PPF-001→TC-PPF-001, REQ-PPF-002→TC-PPF-002,
REQ-PPF-003→TC-PPF-003, REQ-PPF-004→TC-PPF-004

**NPS:** REQ-NPS-001→TC-NPS-001, REQ-NPS-002→TC-NPS-002,
REQ-NPS-003→TC-NPS-003, REQ-NPS-004→TC-NPS-004, REQ-NPS-005→TC-NPS-005

**Salary & Form 16:** REQ-SAL-001→TC-SAL-001, REQ-SAL-002→TC-SAL-002,
REQ-SAL-003→TC-SAL-003, REQ-SAL-004→TC-SAL-004, REQ-SAL-005→TC-SAL-005,
REQ-SAL-006→TC-SAL-006, REQ-SAL-007→TC-SAL-007, REQ-SAL-008→TC-SAL-008,
REQ-SAL-009→TC-SAL-009, REQ-SAL-010→TC-SAL-010, REQ-SAL-011→TC-SAL-011,
REQ-SAL-012→TC-SAL-012

**Income Tax Statements:** REQ-TAX-001→TC-TAX-001,
REQ-TAX-002→TC-TAX-002, REQ-TAX-003→TC-TAX-003, REQ-TAX-004→TC-TAX-004,
REQ-TAX-005→TC-TAX-005, REQ-TAX-006→TC-TAX-006, REQ-TAX-007→TC-TAX-007,
REQ-TAX-008→TC-TAX-008, REQ-TAX-009→TC-TAX-009

**Rental Income:** REQ-RNT-001→TC-RNT-001, REQ-RNT-002→TC-RNT-002,
REQ-RNT-003→TC-RNT-003, REQ-RNT-004→TC-RNT-004, REQ-RNT-005→TC-RNT-005,
REQ-RNT-006→TC-RNT-006

**SGB & RBI Bonds:** REQ-SGB-001→TC-SGB-001, REQ-SGB-002→TC-SGB-002,
REQ-SGB-003→TC-SGB-003, REQ-SGB-004→TC-SGB-004, REQ-SGB-005→TC-SGB-005

**REIT/InvIT:** REQ-REIT-001→TC-REIT-001, REQ-REIT-002→TC-REIT-002,
REQ-REIT-003→TC-REIT-003, REQ-REIT-004→TC-REIT-004,
REQ-REIT-005→TC-REIT-005

**RSU (Phase 2):** REQ-RSU-001→TC-RSU-001, REQ-RSU-002→TC-RSU-002,
REQ-RSU-003→TC-RSU-003, REQ-RSU-004→TC-RSU-004, REQ-RSU-005→TC-RSU-005,
REQ-RSU-006→TC-RSU-006

**ESPP (Phase 2):** REQ-ESPP-001→TC-ESPP-001, REQ-ESPP-002→TC-ESPP-002,
REQ-ESPP-003→TC-ESPP-003, REQ-ESPP-004→TC-ESPP-004,
REQ-ESPP-005→TC-ESPP-005

**DTAA (Phase 2):** REQ-DTAA-001→TC-DTAA-001, REQ-DTAA-002→TC-DTAA-002,
REQ-DTAA-003→TC-DTAA-003, REQ-DTAA-004→TC-DTAA-004

**Schedule FA (Phase 2):** REQ-FA-001→TC-FA-001, REQ-FA-002→TC-FA-002,
REQ-FA-003→TC-FA-003, REQ-FA-004→TC-FA-004

**Unlisted Shares (Phase 2):** REQ-UNL-001→TC-UNL-001,
REQ-UNL-002→TC-UNL-002, REQ-UNL-003→TC-UNL-003, REQ-UNL-004→TC-UNL-004

**Reports & Export:** REQ-RPT-001→TC-RPT-001, REQ-RPT-002→TC-RPT-002,
REQ-RPT-003→TC-RPT-003, REQ-RPT-004→TC-RPT-004, REQ-RPT-005→TC-RPT-005,
REQ-RPT-006→TC-RPT-006, REQ-RPT-007→TC-RPT-007, REQ-RPT-008→TC-RPT-008

*\-\-- End of Test Cases Document v6.0 \-\--*
