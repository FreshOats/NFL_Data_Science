import requests
from bs4 import BeautifulSoup

def scrape_injuries():
    url = "https://www.cbssports.com/nfl/injuries/daily"
    source = requests.get(url)
    soup = BeautifulSoup(source.text, 'html.parser')

    table_data = []
    trs = soup.select('tr.TableBase-bodyTr')

    for tr in trs[1:]:
        row = []
        logo_img = tr.select_one('img.TeamLogo-image')
        if logo_img:
            if logo_img.has_attr('data-lazy'):
                row.append(logo_img['data-lazy'])
            elif logo_img.has_attr('src'):
                row.append(logo_img['src'])
            else:
                row.append('')  # Add empty string if no suitable attribute found
        else:
            row.append('')  # Add empty string if logo not found
        
        # Scrape other data
        row.append(tr.select('td')[0].text)  # Team
        row.append(tr.find('span', class_='CellPlayerName--long').text)  # Player
        row.append(tr.select('td')[2].text)  # Position
        row.append(tr.select('td')[3].text)  # Injury

        table_data.append(row)
    return table_data

# You can add a main block to test the function if needed
if __name__ == "__main__":
    data = scrape_injuries()
    for row in data:
        print(row)
