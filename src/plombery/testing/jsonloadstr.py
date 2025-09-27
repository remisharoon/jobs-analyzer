import json
import unicodedata

def remove_control_characters(s):
    return "".join(ch for ch in s if unicodedata.category(ch)[0]!="C")


jsonstr = """{                    "country": "Saudi Arabia",                    "state": "Makkah",                    "city": "Jeddah",                    "desired tech skills": [                        "AutoCAD 3D",                        "Microsoft Office Suite",                        "Total stations",                        "Builder levels",                        "Robotics",                        "GPS"                    ],                    "desired soft skills": [                        "Ability to work in an environment with people of different backgrounds, races and nationalities",                        "Ability to work full-time variable shifts including days, evenings, nights, and willing to work additional or irregular hours, as needed"                    ],                    "desired domain skills": [],                    "domains": [],                    "company sector": null,                    "position seniority level": "Senior",                    "job type": null,                    "job title": "Survey Engineer",                    "job description": "Verify and record contractor site activities for compliance with Issue for Construction (IFC) design and approved shop drawings. 10+ years of project survey gathering for precise measurements geographic, topographic ad hydrographic features in construction of infrastructure projects, including buildings, pipelines, ICT, roads, tunnels, bridges, HV/MV powerlines, and fiber optic.",                    "job requirements": [                        "Degree in survey engineering, or equivalent",                        "Speak and write English fluently"                    ],                    "job responsibilities": [],                    "job benefits": [],                    "salary": null,                    "company name": "Eram Talent",                    "company description": null,                    "company website": "https://sa.linkedin.com/company/eramtalent",                    "company size": null,                    "company industry": null,                    "company headquarters": null,                    "company employees": null,                    "company revenue": null                    }"""

jsonstr = remove_control_characters(jsonstr)

print(jsonstr)

jsonobj = json.loads(jsonstr)

print(jsonobj)