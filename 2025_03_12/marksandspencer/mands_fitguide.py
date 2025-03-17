import requests
from parsel import Selector

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.marksandspencer.com/cotton-rich-textured-cardigan/p/clp60728440?color=NATURAL',
    'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'iframe',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

def get_cell_text(cell):
    """
    Returns the text content inside a <span> if available,
    otherwise returns the cell's full text.
    """
    span_text = cell.css("span::text").get()
    if span_text:
        return span_text.strip()
    return cell.xpath("string(.)").get().strip()

def parse_table(table):
    """
    Convert a single HTML table element into a dictionary.
    It extracts text from inside <span> tags if available.
    """
    rows = table.css("tr")
    table_dict = {}

    # Check if any header cells (<th>) exist in the first row
    headers_in_row = rows[0].css("th")
    if headers_in_row:
        headers = [get_cell_text(th) for th in headers_in_row]
        table_dict["headers"] = set(headers)
        table_dict["rows"] = []
        for row in rows[1:]:
            cells = row.css("td, th")
            row_dict = {}
            for idx, cell in enumerate(cells):
                key = headers[idx] if idx < len(headers) else f"col{idx+1}"
                row_dict[key] = get_cell_text(cell)
            table_dict["rows"].append(row_dict)
    else:
        # If no headers, simply convert each row into a set of cell texts.
        table_dict["rows"] = []
        for row in rows:
            cells = row.css("td, th")
            cell_texts = {get_cell_text(cell) for cell in cells if get_cell_text(cell)}
            if cell_texts:
                table_dict["rows"].append(cell_texts)

    return table_dict

def convert_all_tables(html_content):
    """
    Find all tables in the given HTML content and convert each to a dictionary.
    Uses the table's title attribute or <caption> text if available.
    Returns a dictionary where each key is the table's title.
    """
    sel = Selector(html_content)
    tables = sel.css("table")
    all_tables = {}
    for idx, table in enumerate(tables):
        title = table.attrib.get("title")
        if not title:
            caption = table.css("caption::text").get()
            title = caption.strip() if caption else f"Table_{idx+1}"
        all_tables[title] = parse_table(table)
    return all_tables

if __name__ == "__main__":
    url = 'https://www.marksandspencer.com/browse/asset/size-guide/en-gb/PDP-SG_SG-WWTOPST38.html'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        table_details = convert_all_tables(response.text)
        # Print out the converted tables dictionary
        for table_title, table_data in table_details.items():
            print(f"{table_data}\n")
    else:
        print("Failed to retrieve the page. Status code:", response.status_code)
