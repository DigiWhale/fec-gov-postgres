
-- https://www.fec.gov/campaign-finance-data/committee-master-file-description/

CREATE TABLE IF NOT EXISTS committee_master (
    cmte_id text NOT NULL, -- CMTE_ID,Committee identification,1,N,VARCHAR2(9),A 9-character alpha-numeric code assigned to a committee by the Federal Election Commission. Committee IDs are unique and an ID for a specific committee always remains the same.,C00100005
    cmte_nm text, -- CMTE_NM,Committee name,2,Y,VARCHAR2(200),,Martha Washington for Congress
    tres_nm text, -- TRES_NM,Treasurer's name,3,Y,VARCHAR2(90),The officially registered treasurer for the committee.,Alexander Hamilton
    cmte_st1 text, -- CMTE_ST1,Street one,4,Y,VARCHAR2(34),,1001 George Washington Hwy
    cmte_st2 text, -- CMTE_ST2,Street two,5,Y,VARCHAR2(34),,Suite 203
    cmte_city text, -- CMTE_CITY,City or town,6,Y,VARCHAR2(30),,Alexandria
    cmte_st text, -- CMTE_ST,State,7,Y,VARCHAR2(2),,VA
    cmte_zip text, -- CMTE_ZIP,ZIP code,8,Y,VARCHAR2(9),,22201
    cmte_dsgn text, -- CMTE_DSGN,Committee designation,9,Y,VARCHAR2(1),A = Authorized by a candidate B = Lobbyist/Registrant PAC D = Leadership PAC J = Joint fundraiser P = Principal campaign committee of a candidate U = Unauthorized,A
    cmte_tp text, -- CMTE_TP,Committee type,10,Y,VARCHAR2(1),List of committee type codes,H
    cmte_pty_affiliation text, -- CMTE_PTY_AFFILIATION,Committee party,11,Y,VARCHAR2(3),List of party codes,NON
    cmte_filing_freq text, -- CMTE_FILING_FREQ,Filing frequency,12,Y,VARCHAR2(1),A = Administratively terminated D = Debt M = Monthly filer Q = Quarterly filer T = Terminated W = Waived,Q
    org_tp text, -- ORG_TP,Interest group category,13,Y,VARCHAR2(1),C = Corporation L = Labor organization M = Membership organization T = Trade association V = Cooperative W = Corporation without capital stock,C
    connected_org_nm text, -- CONNECTED_ORG_NM,Connected organization's name,14,Y,VARCHAR2(200),,Widgets, Incorporated
    cand_id text,  -- CAND_ID,Candidate identification,15,Y,VARCHAR2(9),When a committee has a committee type designation of H, S, or P, the candidate's identification number will be entered in this field.,H1VA01225
    file_year INTEGER,
    PRIMARY KEY (cmte_id, file_year)
);
