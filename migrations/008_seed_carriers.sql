-- Hermes Migration 008: Seed Carriers & State Config
-- Seeds hermes_carriers with top 50 US commercial insurance carriers
-- Seeds hermes_state_config for all 50 states + DC

-- ── Carrier Seed Data ─────────────────────────────────────────────
-- Columns: naic_code, legal_name, group_name, group_naic_code,
--          am_best_rating, domicile_state, company_type,
--          treasury_570_listed, status

INSERT INTO hermes_carriers (
    naic_code, legal_name, group_name, group_naic_code,
    am_best_rating, domicile_state, company_type,
    treasury_570_listed, status
) VALUES

-- ── Tier 1: Treasury 570-listed, largest commercial writers ──────

-- 1. Travelers
('25658', 'Travelers Indemnity Co',
 'Travelers Group', '25658',
 'A++', 'CT', 'stock', TRUE, 'active'),

-- 2. Hartford
('19682', 'Hartford Fire Insurance Co',
 'Hartford Insurance Group', '19682',
 'A+', 'CT', 'stock', TRUE, 'active'),

-- 3. Liberty Mutual
('23035', 'Liberty Mutual Fire Insurance Co',
 'Liberty Mutual Group', '23035',
 'A', 'WI', 'mutual', TRUE, 'active'),

-- 4. Zurich
('16535', 'Zurich American Insurance Co',
 'Zurich Insurance Group', '16535',
 'A+', 'NY', 'stock', TRUE, 'active'),

-- 5. CNA / Continental Casualty
('20443', 'Continental Casualty Co',
 'CNA Financial Group', '20443',
 'A', 'IL', 'stock', TRUE, 'active'),

-- 6. Chubb / ACE American
('22667', 'ACE American Insurance Co',
 'Chubb Group of Insurance Companies', '22667',
 'A++', 'PA', 'stock', TRUE, 'active'),

-- 7. AIG / American Home Assurance
('19380', 'American Home Assurance Co',
 'AIG Property Casualty Group', '19380',
 'A', 'NY', 'stock', TRUE, 'active'),

-- 8. Hanover Insurance Co
('22292', 'Hanover Insurance Co',
 'Hanover Insurance Group', '22292',
 'A', 'NH', 'stock', TRUE, 'active'),

-- 9. Nationwide Mutual
('23787', 'Nationwide Mutual Insurance Co',
 'Nationwide Group', '23787',
 'A+', 'OH', 'mutual', TRUE, 'active'),

-- 10. Erie Insurance Exchange
('26263', 'Erie Insurance Exchange',
 'Erie Insurance Group', '26263',
 'A+', 'PA', 'reciprocal', TRUE, 'active'),

-- 11. Auto-Owners Insurance
('18988', 'Auto-Owners Insurance Co',
 'Auto-Owners Group', '18988',
 'A++', 'MI', 'mutual', TRUE, 'active'),

-- 12. Cincinnati Insurance
('10677', 'Cincinnati Insurance Co',
 'Cincinnati Financial Group', '10677',
 'A+', 'OH', 'stock', TRUE, 'active'),

-- 13. Church Mutual Insurance
('18767', 'Church Mutual Insurance Co',
 'Church Mutual Group', '18767',
 'A-', 'WI', 'mutual', TRUE, 'active'),

-- 14. Employers Mutual Casualty
('21415', 'Employers Mutual Casualty Co',
 'EMC Insurance Companies', '21415',
 'A', 'IA', 'mutual', TRUE, 'active'),

-- 15. Federated Mutual
('13935', 'Federated Mutual Insurance Co',
 'Federated Insurance Group', '13935',
 'A+', 'MN', 'mutual', TRUE, 'active'),

-- 16. Frankenmuth Mutual
('13986', 'Frankenmuth Mutual Insurance Co',
 'Frankenmuth Insurance Group', '13986',
 'A', 'MI', 'mutual', TRUE, 'active'),

-- 17. Grinnell Mutual Reinsurance
('14117', 'Grinnell Mutual Reinsurance Co',
 'Grinnell Mutual Group', '14117',
 'A', 'IA', 'mutual', TRUE, 'active'),

-- 18. AMERITAS Life Insurance
('61689', 'AMERITAS Life Insurance Corp',
 'AMERITAS Life Partners Group', '61689',
 'A', 'NE', 'mutual', TRUE, 'active'),

-- 19. Berkshire Hathaway / National Indemnity
('20087', 'National Indemnity Co',
 'Berkshire Hathaway Insurance Group', '20087',
 'A++', 'NE', 'stock', TRUE, 'active'),

-- 20. Markel Insurance
('38970', 'Markel Insurance Co',
 'Markel Group', '38970',
 'A', 'IL', 'stock', TRUE, 'active'),

-- 21. Tokio Marine / Philadelphia Indemnity
('18058', 'Philadelphia Indemnity Insurance Co',
 'Tokio Marine Group', '18058',
 'A++', 'PA', 'stock', TRUE, 'active'),

-- 22. Great American Insurance
('16691', 'Great American Insurance Co',
 'Great American Insurance Group', '16691',
 'A+', 'OH', 'stock', TRUE, 'active'),

-- 23. State Auto Insurance
('25135', 'State Auto Property & Casualty Insurance Co',
 'State Auto Group', '25135',
 'A-', 'OH', 'stock', TRUE, 'active'),

-- 24. Westfield Insurance
('24112', 'Westfield Insurance Co',
 'Westfield Group', '24112',
 'A', 'OH', 'mutual', TRUE, 'active'),

-- 25. ICW Group / Insurance Company of the West
('27847', 'Insurance Co of the West',
 'ICW Group', '27847',
 'A-', 'CA', 'mutual', TRUE, 'active'),

-- ── Tier 2: Major regional & specialty commercial writers ────────

-- 26. EMPLOYERS Holdings / Employers Insurance of NV
('10640', 'Employers Insurance Co of Nevada',
 'EMPLOYERS Holdings Group', '10640',
 'A-', 'NV', 'stock', FALSE, 'active'),

-- 27. Sentry Insurance
('21180', 'Sentry Insurance a Mutual Co',
 'Sentry Insurance Group', '21180',
 'A+', 'WI', 'mutual', FALSE, 'active'),

-- 28. Society Insurance
('14176', 'Society Insurance, a Mutual Co',
 'Society Insurance Group', '14176',
 'A', 'WI', 'mutual', FALSE, 'active'),

-- 29. Selective Insurance
('26301', 'Selective Insurance Co of America',
 'Selective Insurance Group', '26301',
 'A+', 'NJ', 'stock', FALSE, 'active'),

-- 30. Donegal Mutual Insurance
('14303', 'Donegal Mutual Insurance Co',
 'Donegal Insurance Group', '14303',
 'A', 'PA', 'mutual', FALSE, 'active'),

-- 31. AMERISURE Insurance
('19488', 'AMERISURE Insurance Co',
 'AMERISURE Companies', '19488',
 'A', 'MI', 'mutual', FALSE, 'active'),

-- 32. Builders Mutual Insurance
('41394', 'Builders Mutual Insurance Co',
 'Builders Mutual Group', '41394',
 'A', 'NC', 'mutual', FALSE, 'active'),

-- 33. BITCO General Insurance
('20095', 'BITCO General Insurance Corp',
 'BITCO Insurance Companies', '20095',
 'A+', 'IL', 'stock', FALSE, 'active'),

-- 34. Acuity Insurance
('14184', 'Acuity, a Mutual Insurance Co',
 'Acuity Insurance Group', '14184',
 'A+', 'WI', 'mutual', FALSE, 'active'),

-- 35. West Bend Mutual
('15350', 'West Bend Mutual Insurance Co',
 'West Bend Mutual Group', '15350',
 'A+', 'WI', 'mutual', FALSE, 'active'),

-- 36. Merchants Insurance Group
('14206', 'Merchants Insurance Co of New Hampshire Inc',
 'Merchants Insurance Group', '14206',
 'A-', 'NH', 'stock', FALSE, 'active'),

-- 37. RLI Insurance
('13056', 'RLI Insurance Co',
 'RLI Insurance Group', '13056',
 'A+', 'IL', 'stock', FALSE, 'active'),

-- 38. Kinsale Capital Group
('38920', 'Kinsale Insurance Co',
 'Kinsale Capital Group', '38920',
 'A-', 'VA', 'stock', FALSE, 'active'),

-- 39. James River Insurance (E&S)
('12203', 'James River Insurance Co',
 'James River Group Holdings', '12203',
 'A-', 'OH', 'stock', FALSE, 'active'),

-- 40. Argo Group / Colony Insurance (primary US admitted entity)
-- Note: NAIC 11150 belongs to Arch; Argo's US admitted carrier is Colony Insurance (19879)
('19879', 'Colony Insurance Co',
 'Argo Group', '19879',
 'A-', 'VA', 'stock', FALSE, 'active'),

-- 41. STARR Indemnity & Liability
('38318', 'STARR Indemnity & Liability Co',
 'Starr Insurance Companies', '38318',
 'A', 'TX', 'stock', FALSE, 'active'),

-- 42. Everest Re Group
('26921', 'Everest National Insurance Co',
 'Everest Re Group', '26921',
 'A+', 'DE', 'stock', FALSE, 'active'),

-- 43. W.R. Berkley / Berkley Insurance
('32603', 'Berkley Insurance Co',
 'W.R. Berkley Insurance Group', '32603',
 'A+', 'DE', 'stock', FALSE, 'active'),

-- 44. Arch Insurance Co (NAIC 11150 is the correct Arch Insurance Co code)
('11150', 'Arch Insurance Co',
 'Arch Capital Group', '11150',
 'A+', 'MO', 'stock', FALSE, 'active'),

-- 45. Allied World Assurance
('19489', 'Allied World Insurance Co',
 'Allied World Assurance Group', '19489',
 'A', 'NH', 'stock', FALSE, 'active'),

-- 46. Sompo International / Endurance American
('10641', 'Endurance American Insurance Co',
 'Sompo International Holdings', '10641',
 'A+', 'DE', 'stock', FALSE, 'active'),

-- 47. Allianz / Fireman's Fund
('21873', 'Fireman''s Fund Insurance Co',
 'Allianz Insurance Group', '21873',
 'A+', 'CA', 'stock', FALSE, 'active'),

-- 48. QBE Insurance Corporation
('39217', 'QBE Insurance Corp',
 'QBE Insurance Group', '39217',
 'A', 'NY', 'stock', FALSE, 'active'),

-- 49. Aspen Insurance Co
('43460', 'Aspen Insurance Ltd',
 'Aspen Insurance Holdings', '43460',
 'A', 'NY', 'stock', FALSE, 'active'),

-- 50. USAA / Garrison Property & Casualty
('21253', 'Garrison Property and Casualty Insurance Co',
 'USAA Group', '21253',
 'A++', 'TX', 'reciprocal', FALSE, 'active')

ON CONFLICT (naic_code) DO NOTHING;

-- ── State Config Seed Data ────────────────────────────────────────
-- Tier 1: TX, CA, FL, NY, IL          (scrape_enabled = TRUE)
-- Tier 2: PA, OH, GA, NC, NJ, VA, MI, MA
-- Tier 3: All remaining states + DC

INSERT INTO hermes_state_config (
    state, state_name, sfa_portal_url, sfa_accessible,
    tier, lines_available, scrape_enabled
) VALUES

-- ── Tier 1 ───────────────────────────────────────────────────────
('TX', 'Texas',
 'https://filingaccess.serff.com/sfa/home/TX', TRUE, 1,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 TRUE),
('CA', 'California',
 'https://filingaccess.serff.com/sfa/home/CA', TRUE, 1,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 TRUE),
('FL', 'Florida',
 'https://filingaccess.serff.com/sfa/home/FL', TRUE, 1,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 TRUE),
('NY', 'New York',
 'https://filingaccess.serff.com/sfa/home/NY', TRUE, 1,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 TRUE),
('IL', 'Illinois',
 'https://filingaccess.serff.com/sfa/home/IL', TRUE, 1,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 TRUE),

-- ── Tier 2 ───────────────────────────────────────────────────────
('PA', 'Pennsylvania',
 'https://filingaccess.serff.com/sfa/home/PA', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('OH', 'Ohio',
 'https://filingaccess.serff.com/sfa/home/OH', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('GA', 'Georgia',
 'https://filingaccess.serff.com/sfa/home/GA', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('NC', 'North Carolina',
 'https://filingaccess.serff.com/sfa/home/NC', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('NJ', 'New Jersey',
 'https://filingaccess.serff.com/sfa/home/NJ', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('VA', 'Virginia',
 'https://filingaccess.serff.com/sfa/home/VA', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('MI', 'Michigan',
 'https://filingaccess.serff.com/sfa/home/MI', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('MA', 'Massachusetts',
 'https://filingaccess.serff.com/sfa/home/MA', TRUE, 2,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),

-- ── Tier 3 ───────────────────────────────────────────────────────
('AL', 'Alabama',
 'https://filingaccess.serff.com/sfa/home/AL', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('AK', 'Alaska',
 'https://filingaccess.serff.com/sfa/home/AK', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('AZ', 'Arizona',
 'https://filingaccess.serff.com/sfa/home/AZ', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('AR', 'Arkansas',
 'https://filingaccess.serff.com/sfa/home/AR', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('CO', 'Colorado',
 'https://filingaccess.serff.com/sfa/home/CO', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('CT', 'Connecticut',
 'https://filingaccess.serff.com/sfa/home/CT', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('DE', 'Delaware',
 'https://filingaccess.serff.com/sfa/home/DE', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('DC', 'District of Columbia',
 'https://filingaccess.serff.com/sfa/home/DC', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('HI', 'Hawaii',
 'https://filingaccess.serff.com/sfa/home/HI', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('ID', 'Idaho',
 'https://filingaccess.serff.com/sfa/home/ID', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('IN', 'Indiana',
 'https://filingaccess.serff.com/sfa/home/IN', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('IA', 'Iowa',
 'https://filingaccess.serff.com/sfa/home/IA', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('KS', 'Kansas',
 'https://filingaccess.serff.com/sfa/home/KS', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('KY', 'Kentucky',
 'https://filingaccess.serff.com/sfa/home/KY', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('LA', 'Louisiana',
 'https://filingaccess.serff.com/sfa/home/LA', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('ME', 'Maine',
 'https://filingaccess.serff.com/sfa/home/ME', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('MD', 'Maryland',
 'https://filingaccess.serff.com/sfa/home/MD', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('MN', 'Minnesota',
 'https://filingaccess.serff.com/sfa/home/MN', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('MS', 'Mississippi',
 'https://filingaccess.serff.com/sfa/home/MS', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('MO', 'Missouri',
 'https://filingaccess.serff.com/sfa/home/MO', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('MT', 'Montana',
 'https://filingaccess.serff.com/sfa/home/MT', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('NE', 'Nebraska',
 'https://filingaccess.serff.com/sfa/home/NE', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('NV', 'Nevada',
 'https://filingaccess.serff.com/sfa/home/NV', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('NH', 'New Hampshire',
 'https://filingaccess.serff.com/sfa/home/NH', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('NM', 'New Mexico',
 'https://filingaccess.serff.com/sfa/home/NM', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('ND', 'North Dakota',
 'https://filingaccess.serff.com/sfa/home/ND', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('OK', 'Oklahoma',
 'https://filingaccess.serff.com/sfa/home/OK', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('OR', 'Oregon',
 'https://filingaccess.serff.com/sfa/home/OR', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('RI', 'Rhode Island',
 'https://filingaccess.serff.com/sfa/home/RI', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('SC', 'South Carolina',
 'https://filingaccess.serff.com/sfa/home/SC', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('SD', 'South Dakota',
 'https://filingaccess.serff.com/sfa/home/SD', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('TN', 'Tennessee',
 'https://filingaccess.serff.com/sfa/home/TN', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('UT', 'Utah',
 'https://filingaccess.serff.com/sfa/home/UT', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('VT', 'Vermont',
 'https://filingaccess.serff.com/sfa/home/VT', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('WA', 'Washington',
 'https://filingaccess.serff.com/sfa/home/WA', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('WV', 'West Virginia',
 'https://filingaccess.serff.com/sfa/home/WV', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('WI', 'Wisconsin',
 'https://filingaccess.serff.com/sfa/home/WI', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE),
('WY', 'Wyoming',
 'https://filingaccess.serff.com/sfa/home/WY', TRUE, 3,
 ARRAY['commercial_property','general_liability','commercial_auto','workers_comp','umbrella_excess'],
 FALSE)

ON CONFLICT (state) DO NOTHING;
