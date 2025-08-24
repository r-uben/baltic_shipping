# Baltic Shipping Vessel Data Format

## Overview
Each vessel record contains up to **27 fields** of information scraped from Baltic Shipping website.

## Data Fields by Category

### 1. Vessel Identification (5 fields)
| Field | Description | Example |
|-------|-------------|---------|
| **IMO number** | Unique 7-digit International Maritime Organization number | `9872365` |
| **MMSI** | Maritime Mobile Service Identity (9-digit radio callsign) | `253676000` |
| **Vessel name** | Current vessel name with IMO | `GALILEO GALILEI, IMO 9872365` |
| **Name of the ship** | Clean vessel name | `GALILEO GALILEI` |
| **Former names** | Previous names with years | `TIAN YU (2020)` |

### 2. Vessel Classification (4 fields)
| Field | Description | Example |
|-------|-------------|---------|
| **Vessel type** | Ship category/purpose | `Dredger`, `Pipe laying vessel`, `Yacht` |
| **Operating status** | Current operational state | `Active`, `Being built`, `Laid up` |
| **Flag** | Country of registration | `Luxembourg`, `Malta`, `United Kingdom (UK)` |
| **Year of build** | Construction year | `2020` |

### 3. Physical Dimensions (4 fields)
| Field | Description | Example |
|-------|-------------|---------|
| **Gross tonnage** | Total internal volume | `22000 tons` |
| **Deadweight** | Maximum cargo capacity | `30000 tons` |
| **Length** | Ship length | `167 m` |
| **Breadth** | Ship width/beam | `36 m` |
| **Draft** | Depth below waterline | `10 m` |

### 4. Technical Specifications (3 fields)
| Field | Description | Example |
|-------|-------------|---------|
| **Engine type** | Main engine manufacturer | `Wartsila` |
| **Engine model** | Specific engine model | `12V UD25M5` |
| **Engine power** | Total engine output | `51480 KW` |

### 5. Ownership & Management (5 fields)
| Field | Description | Example |
|-------|-------------|---------|
| **Owner** | Vessel owner company | `ALLSEAS ENGINEERING - DELFT, NETHERLANDS` |
| **Manager** | Management company | `SARNIA YACHTS - ST. PETERS PORT (GUERNSEY)` |
| **Builder** | Shipyard that built vessel | `MITSUBISHI HEAVY INDUSTRIES LTD - KOBE, JAPAN` |
| **Home port** | Port of registry | `LONDON`, `NASSAU` |
| **Classification society** | Safety/standards organization | `LLOYD'S SHIPPING REGISTER` |

### 6. Additional Information (6 fields)
| Field | Description | Example |
|-------|-------------|---------|
| **Description** | Auto-generated summary | `GALILEO GALILEI is a Dredger built in 2020...` |
| **Seafarers worked on** | Employment records | `5 service records found` |
| **Open vacancies on** | Job postings | `No open vacancies on this ship` |
| **Vessel MLC insurance** | Maritime Labour Convention insurance | `Search` |
| **source_url** | Baltic Shipping URL | `https://www.balticshipping.com/vessel/imo/9872365` |
| **Clear all** | UI element (can be ignored) | `Close` |

## Data Format Examples

### Complete Vessel Record (SOLITAIRE)
```
IMO number: 7129049
MMSI: 249118000
Vessel name: SOLITAIRE, IMO 7129049
Vessel type: Pipe laying vessel
Operating status: Active
Flag: Malta
Gross tonnage: 94855 tons
Deadweight: 127435 tons
Length: 299 m
Breadth: 40 m
Year of build: 1972
Builder: MITSUBISHI HEAVY INDUSTRIES LTD - KOBE, JAPAN
Classification society: LLOYD'S SHIPPING REGISTER
Owner: ALLSEAS ENGINEERING - DELFT, NETHERLANDS
Manager: ALLSEAS ENGINEERING - DELFT, NETHERLANDS
Engine type: Wartsila
Engine model: 12V UD25M5
Engine power: 51480 KW
Former names: SOLITAIRE (2015), SOLITAIRE I (1993), COMSHIP (1992), AKDENIZ S (1992), TRENTWOOD (1990)
```

### Minimal Vessel Record
Some vessels may have limited information:
```
IMO number: 8213744
Vessel name: [Vessel name from page]
Operating status: Unknown
Flag: Unknown
```

## Data Quality Notes

1. **Not all fields are populated** for every vessel
2. **Former names** include years in parentheses
3. **Tonnage and dimensions** include units (tons, m, KW)
4. **Company names** often include location
5. **Some fields** like "Clear all" and "Vessel MLC insurance" are UI elements that can be filtered out

## CSV Output Format

The data is saved as CSV with:
- **Delimiter**: Comma (`,`)
- **Encoding**: UTF-8
- **Headers**: First row contains field names
- **Empty values**: Blank cells for missing data
- **Special characters**: Properly escaped in quotes

## Usage for Analysis

Key fields for analysis:
- **IMO number**: Primary unique identifier
- **Vessel type**: For categorization
- **Flag**: For geographic analysis
- **Year of build**: For age analysis
- **Gross tonnage/Deadweight**: For size analysis
- **Operating status**: For fleet activity analysis

## Data Completeness by Vessel Type

From testing, completeness varies:
- **Large commercial vessels**: Usually complete data
- **Specialty vessels** (pipe laying, dredgers): Good technical data
- **Older vessels**: May lack engine specifications
- **Yachts**: Often missing technical details