import requests
from bs4 import BeautifulSoup
import markdown as md

def extract_markdown_from_orca_sightings():
    # Send a GET request to orcanetwork.org
    response = requests.get("https://www.orcanetwork.org/recent-sightings")
    html_content = response.content

    # Use Beautiful Soup to parse the HTML
    soup = BeautifulSoup(html_content, "html.parser")

    element = soup.find_all('div', class_='sqs-html-content')
    e = element[3]

    all_content = e.contents  
    res = ""
    md_list = []

    for content in all_content:
        res += str(content)
        mark_d = md(str(content))
        if mark_d.strip() == "" or '-***' not in mark_d:
            continue
        md_list.append(mark_d)

    return md_list


markdown_list = extract_markdown_from_orca_sightings()
