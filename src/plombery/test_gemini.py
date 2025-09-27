import requests
from config import read_config


# payload = """
# {'contents': [{'parts': [{'text': "  Extract and return these fields in a dictionary:\n                    1. country\n                    2.state\n                    3.city\n                    4.desired tech skills (as a list)\n                    5.desired soft skills (as a list)\n                    6.desired domain skills (as a list)\n                    7. domains (as a list)\n                    8.company sector\n                    9.position seniority level\n                    10. job type\n                    11. job title\n                    12. job description\n                    13. job requirements\n                    14. job responsibilities\n                    15. job benefits\n                    16. salary (if mentioned)\n                    18. company name (if mentioned)\n                    19. company description (if mentioned)\n                    20. company website (if mentioned)\n                    21. company size (if mentioned)\n                    22. company industry (if mentioned)\n                    23. company headquarters (if mentioned)\n                    24. company employees (if mentioned)\n                    25. company revenue (if mentioned)\n                    , from this text  - Junior Software Engineer/Full-Stack Developer Transgulf Readymix Dubai, Dubai, United Arab Emirates We are seeking a motivated individual to join our growing team. You will work across the full software development lifecycle, contributing to both front-end and back-end systems. This role is ideal for someone passionate about clean code, intuitive design, and robust data handling. **Key Responsibilities:** * Design and develop responsive web interfaces using ASP.NET, JavaScript, CSS, and HTML * Collaborate on UI/UX design to create user-friendly experiences * Build and maintain back-end services and integrate with relational databases * Perform data extraction, transformation, and preparation for various applications * Write and optimize SQL queries for performance and reliability * Participate in code reviews, testing, and deployment activities **Requirements:** * Bachelor's degree in Computer Science, Engineering, or a related field * 0-3 years experience. * Strong proficiency in SQL is a must * Experience with ASP.NET, JavaScript, HTML, CSS * Understanding of UI/UX principles and responsive design * Familiarity with back-end development and relational database systems * Eagerness to learn and work in a collaborative, fast-paced environment https://ae.linkedin.com/company/tgrmcc "}]}]}
# """

input_text = "Junior Software Engineer/Full-Stack Developer Transgulf Readymix Dubai, Dubai, United Arab Emirates We are seeking a motivated individual to join our growing team. You will work across the full software development lifecycle, contributing to both front-end and back-end systems. This role is ideal for someone passionate about clean code, intuitive design, and robust data handling. **Key Responsibilities:** * Design and develop responsive web interfaces using ASP.NET, JavaScript, CSS, and HTML * Collaborate on UI/UX design to create user-friendly experiences * Build and maintain back-end services and integrate with relational databases * Perform data extraction, transformation, and preparation for various applications * Write and optimize SQL queries for performance and reliability * Participate in code reviews, testing, and deployment activities **Requirements:** * Bachelor's degree in Computer Science, Engineering, or a related field * 0-3 years experience. * Strong proficiency in SQL is a must * Experience with ASP.NET, JavaScript, HTML, CSS * Understanding of UI/UX principles and responsive design * Familiarity with back-end development and relational database systems * Eagerness to learn and work in a collaborative, fast-paced environment https://ae.linkedin.com/company/tgrmcc"

payload = {
    "contents": [{"parts": [{"text": f"""  Extract and return these fields in a dictionary:
            1. country
            2.state
            3.city
            4.desired tech skills (as a list)
            5.desired soft skills (as a list)
            6.desired domain skills (as a list)
            7. domains (as a list)
            8.company sector
            9.position seniority level
            10. job type
            11. job title
            12. job description
            13. job requirements
            14. job responsibilities
            15. job benefits
            16. salary (if mentioned)
            18. company name (if mentioned)
            19. company description (if mentioned)
            20. company website (if mentioned)
            21. company size (if mentioned)
            22. company industry (if mentioned)
            23. company headquarters (if mentioned)
            24. company employees (if mentioned)
            25. company revenue (if mentioned)
            , from this text  - {input_text} """}]}]
}

headers = """
{'Content-Type': 'application/json', 'X-goog-api-key': 'AIzaSyCXRwbR5mXY68DNJmepu2KIqYgQeTBKA-Q'}
"""

url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"



# params = {'key': 'your_api_key'}  # Replace 'your_api_key' with the actual API key
gemini_config = read_config()['GeminiPro']
API_KEY = gemini_config['API_KEY']

headers = {
    'Content-Type': 'application/json',
    'X-goog-api-key': API_KEY
}

params = {'key': API_KEY}  # Use the actual API key provided

response = requests.post(url, json=payload, headers=headers, params=params)


print(response.text, response.status_code, response.headers, response.url, response.content)