import requests
from bs4 import BeautifulSoup
import pandas as pd
import ndjson

from tqdm import tqdm
tqdm.pandas()

from pandarallel import pandarallel
pandarallel.initialize(verbose=0, progress_bar=True)

from requests.packages import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {"User-Agent": "Mozilla/5.0 (Linux; U; Android 4.2.2; he-il; NEO-X5-116A Build/JDQ39) AppleWebKit/534.30 ("
                         "KHTML, like Gecko) Version/4.0 Safari/534.30"}

def getStatement(url):
    global headers
    response = requests.get(url, headers=headers, verify=False)
    webpage = BeautifulSoup(response.content, "html.parser")
    webpage = webpage.find('div', {'id': 'content_right'})
    statements = webpage.find_all('p')
    text = ""
    done = False
    for statement in statements:
        if done:
            text = text + '\n' + statement.text
        elif statement.text.find('Last Statement:') != -1:
            done = True 
    return text[1:]

def getInfo (row):
    global headers, outdir
    url = row['Info Link']
    index = row.name
    try:
        response = requests.get(url, headers=headers, verify=False)
    except:
        print('Couldn\'t fetch details', index, url)
        return {}
    urlTyle = url.split('.')[-1]
    if urlTyle == 'html':
        webpage = BeautifulSoup(response.content, "html.parser")
        dataObj = { 'type': 'webpage' }
        ps = webpage.find_all('p', {'class': None})
        text = ''
        for p in ps:
            span = p.find('span')
            if span == None:
                text += p.text.strip() + '\n'
                continue
            key = span.text
            span.clear()
            value = text + p.text.strip()
            text = ''
            dataObj[key] = value

        webpage = webpage.find('table', {"class": "table_deathrow"})
        if webpage == None:
            print(url, index, 'Table not found')
            return {}
        trs = webpage.find_all('tr')
        for tr in trs:
            tds = tr.find_all('td')
            hasImage = tds[0].find('img') != None
            if hasImage:
                imgUrl = 'https://www.tdcj.texas.gov/death_row/dr_info/' + tds[0].find('img')['src']
                response2 = requests.get(imgUrl, headers=headers, verify=False)
                imgType = imgUrl.split('.')[-1]
                fp = outdir + 'info_images/' + str(index) + '_profile.' + imgType
                with open(fp, 'wb') as f:
                    f.write(response2.content) 
                dataObj['profileImagePath'] = fp
            key = tds[-2].text
            value = tds[-1].text
            dataObj[key] = value
        return dataObj
    else:
        fp = outdir + 'info_images/' + str(index) + '.' + urlTyle
        with open(fp, 'wb') as f:
            f.write(response.content) 
        return {
            'type': 'image',
            'path': fp 
        }

dbUrl = "https://www.tdcj.texas.gov/death_row/dr_executed_offenders.html"

outdir = "data/"

print("Fetching data from URL...")

response = requests.get(dbUrl, headers=headers, verify=False)
webpage = BeautifulSoup(response.content, "html.parser")

webpage = webpage.find('table', {"class": "tdcj_table"})

trs = webpage.find_all('tr')

head = trs[0]
trs = trs[1:]
total = len(trs)

ths = head.find_all('th')
columns = [th.text for th in ths]
columns[1] = "Info " + columns[1]
columns[2] = "Statement " + columns[2]

rows = []
print("Crawling total", total, "Criminals")

for tr in trs:
    tds = tr.find_all('td')
    row = []
    for td in tds:
        link_td = td.find('a')
        if link_td:
            href = link_td['href']
            if href.find('/death_row') != -1:
                row.append("https://www.tdcj.texas.gov" + href)
            else:
                row.append("https://www.tdcj.texas.gov/death_row/" + href)
            
        else:
            row.append(td.text)
    rows.append(row)
    
df = pd.DataFrame(rows, columns=columns)
df = df.set_index('Execution')


df.to_csv(outdir + 'main.csv', encoding='utf-8')

print('Getting User Info...')
df['Info'] = df[['Info Link']].parallel_apply(getInfo, axis=1)

print("Getting Statement Info...")
df['Statement'] = df['Statement Link'].parallel_apply(getStatement)

obj = df.to_dict('index')
with open(outdir + 'main.json', 'w') as f:
    ndjson.dump([obj], f,ensure_ascii=False, indent=4)

df.to_pickle(outdir + 'main_v4.pkl', protocol=4)
df.to_pickle(outdir + 'main.pkl', protocol=5)

print("Crawling Complete!")