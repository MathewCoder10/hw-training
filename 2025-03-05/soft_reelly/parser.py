import requests
from pymongo import MongoClient
from datetime import datetime

class Parser:
    def __init__(self):
        # Initialize MongoDB client and collections
        self.client = MongoClient('mongodb://localhost:27017')
        self.db = self.client['softreelly_db']
        self.parser_collection = self.db['parser']
        self.crawler_collection = self.db['crawler']  
        
        # Set up HTTP headers to be used in requests
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://soft.reelly.io',
            'referer': 'https://soft.reelly.io/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
        # Base URLs for API endpoints
        self.project_base_url = "https://api.reelly.io/api:sk5LT7jx/projects/"
        self.additional_info_url = "https://api.reelly.io/api:sk5LT7jx/get-project-additional-info/"
        self.parkings_url = "https://xdil-qda0-zofk.m2.xano.io/api:sk5LT7jx/parkings"

    def start(self):
        """
        Retrieves project IDs and URLs from the crawler collection, then for each project ID,
        it requests the project details, parses and cleans the data (including the URL),
        merges additional info, and finally stores the combined item into the parser collection.
        """
        for doc in self.crawler_collection.find({}, {"id": 1, "url": 1}):
            if "id" not in doc:
                continue
            project_id = doc["id"]
            project_url = doc.get("url")
            
            url = f"{self.project_base_url}{project_id}"
            response = requests.get(url, headers=self.headers)
            if response.status_code != 200:
                print(f"Error for project id {project_id}: {response.status_code}")
                continue
            data = response.json()
            if not data:
                continue

            # Parse the individual item and pass the crawler URL to be included in the item
            item = self.parse_item(data, project_url)
            
            # Merge additional information from other endpoints
            item.update(self.fetch_project_additional_info(project_id))
            item.update(self.fetch_project_parkings(project_id))
            
            # Insert the combined item into the parser_collection
            self.parser_collection.insert_one(item)
            print(f"Stored data for project {project_id}")

    def parse_item(self, data, crawler_url):
        """
        Extracts and cleans data from the API response and includes the crawler URL.
        """
        # Cleaning function for the overview
        def clean_overview(overview):
            if not overview:
                return ""
            lines = overview.splitlines()
            cleaned_lines = []
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("#####"):
                    continue
                if stripped:
                    cleaned_lines.append(stripped)
            return " ".join(cleaned_lines)

        def convert_timestamp(ts):
            try:
                return datetime.fromtimestamp(ts / 1000).strftime("%b %d, %Y") if ts else None
            except Exception:
                return None

        # Extract facilities
        facility_names = []
        for facility_group in (data.get("Facilities") or []):
            if facility_group:
                for facility in facility_group:
                    if facility and facility.get("Name"):
                        facility_names.append(facility.get("Name"))
        
        # Extract layout images from the Starting_price section
        layout_images = []
        for item in (data.get("Starting_price") or []):
            if item:
                for image in (item.get("Typical_unit_jpg") or []):
                    if image and image.get("url"):
                        layout_images.append(image.get("url"))
        
        # Build combined payment_plans field from Payment_plans information
        payment_plans = []
        for group in (data.get("Payment_plans") or []):
            if group:
                for plan in group:
                    if plan:
                        percent = plan.get("Percent_of_payment", "")
                        payment_time = plan.get("Payment_time", "")
                        payment_plans.append(f'"{percent}-{payment_time}"')
        
        # Build combined map_points field from Map_points information
        map_points_combined = []
        for group in (data.get("Map_points") or []):
            if group:
                for point in group:
                    if point:
                        point_name = point.get("Point_name", "")
                        distance = point.get("Distance_km", "")
                        map_points_combined.append(f'"{point_name}-{distance}"')
        
        # Extract PDF URL for floor plans
        floor_plans_pdf = None
        pdfs = data.get("Units_layouts_PDF") or []
        if pdfs and len(pdfs) > 0 and pdfs[0]:
            floor_plans_pdf = pdfs[0].get("url")
        
        # Extract unit details from Starting_price
        unit_bedrooms = []
        unit_areas = []
        unit_prices = []
        for item in (data.get("Starting_price") or []):
            if item:
                unit_bedrooms.append(item.get("unit_bedrooms"))
                unit_areas.append({
                    "areafromsqft": item.get("Area_from_sqft"),
                    "areatosqft": item.get("Area_to_sqft")
                })
                unit_prices.append({
                    "pricefromAED": item.get("Price_from_AED"),
                    "pricetoAED": item.get("Price_to_AED")
                })
        
        # Combine unit details into the desired format:
        # "unit_bedrooms - areafrom sqft - areatos sqft , AED pricefrom - priceto"
        unit_details_combined = []
        for i in range(len(unit_bedrooms)):
            area = unit_areas[i] if i < len(unit_areas) else {}
            price = unit_prices[i] if i < len(unit_prices) else {}
            area_range = f"{area.get('areafromsqft', '')} sqft - {area.get('areatosqft', '')} sqft"
            price_range = f"AED {price.get('pricefromAED', '')} - AED {price.get('pricetoAED', '')}"
            unit_details_combined.append(f'"{unit_bedrooms[i]} - {area_range} , {price_range}"')
        
        # Map the API's Status to a custom development status
        status = data.get("Status")
        if status == "Under construction":
            development_status = "Construction"
        elif status == "Presale":
            development_status = "Planned"
        elif status == "Completed":
            development_status = "Completed"
        else:
            development_status = None
        
        # Convert timestamp fields
        completion_time = convert_timestamp(data.get("Completion_time"))
        created_at = convert_timestamp(data.get("created_at"))
        last_modified = convert_timestamp(data.get("Last_Modified"))
        
        # Process developer information
        developer_info = data.get("Developer") or []
        if developer_info and isinstance(developer_info, list) and len(developer_info) > 0 and developer_info[0]:
            dev = developer_info[0]
            developer_id = dev.get("id")
            developer_website = dev.get("website")
            logo_images = dev.get("Logo_image") or []
            logo_image_url = logo_images[0].get("url") if logo_images and len(logo_images) > 0 and logo_images[0] else None
        else:
            developer_id = None
            developer_website = None
            logo_image_url = None

        # Clean the overview field using the local cleaning function
        overview_cleaned = clean_overview(data.get("Overview"))

        # Build and return the cleaned item dictionary with the new combined fields,
        # including the "url" field from the crawler collection.
        item = {
            "id": data.get("id"),
            "url": crawler_url,
            "developer_ids": developer_id,
            "developer_names": data.get("Developers_name"),
            "developer_websites": developer_website,
            "developer_logo": logo_image_url,
            "project_name": data.get("Project_name"),
            "completion_date": data.get("Completion_date"),
            "completion_time": completion_time,
            "created_at": created_at,
            "last_modified": last_modified,
            "coordinates": data.get("Coordinates"),
            "area_name": data.get("Area_name"),
            "region": data.get("Region"),
            "development_status": development_status,
            "sale_status": data.get("sale_status"),
            "overview": overview_cleaned,
            "floors": data.get("Floors"),
            "furnishing": data.get("Furnishing"),
            "units_types": ', '.join([f'"{itm}"' for itm in (data.get("Units_types") or [])]),
            "minimum_price": data.get("min_price"),
            "cover_image_url": (data.get("cover") or {}).get("url"),
            "architecture_images": ', '.join([f'"{itm.get("url")}"' for itm in (data.get("Architecture") or []) if itm and itm.get("url")]),
            "interior_images": ', '.join([f'"{itm.get("url")}"' for itm in (data.get("Interior") or []) if itm and itm.get("url")]),
            "lobby_images": ', '.join([f'"{itm.get("url")}"' for itm in (data.get("Lobby") or []) if itm and itm.get("url")]),
            "facility_names": ', '.join([f'"{facility}"' for facility in facility_names]),
            "service_charge": data.get("Service_Charge"),
            "layout_images": ', '.join([f'"{url}"' for url in layout_images]),
            "payment_plans": ', '.join(payment_plans),
            "map_points": ', '.join(map_points_combined),
            "unit_bedrooms": ', '.join(unit_details_combined),
            "floor_plans_pdf": floor_plans_pdf,
            "marketing_brochure": data.get("Brochure")            
        }
        return item

    def fetch_project_additional_info(self, project_id):
        """
        Fetches additional project information (e.g. total number of units) from a separate API endpoint.
        """
        url = f"{self.additional_info_url}{project_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            total_units = sum(item.get("Units_amount", 0) for item in (data.get("inventory") or []))
            return {"total_units": total_units}
        else:
            print(f"Error fetching additional info for project id {project_id}: {response.status_code}")
            print("Response text:", response.text)
            return {}

    def fetch_project_parkings(self, project_id):
        """
        Fetches parking space information for the project.
        Formats the data and then cleans it to store as a string rather than a list.
        """
        # Cleaning function for the number_of_parking field
        def clean_number_of_parkings(parking_data):
            if not parking_data:
                return ""
            if isinstance(parking_data, list):
                if len(parking_data) == 0:
                    return ""
                return ", ".join(parking_data)
            return parking_data

        params = {
            'project_id': str(project_id)
        }
        response = requests.get(self.parkings_url, params=params, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            parking_values = []
            for item in data:
                if item:
                    unit_bedrooms = item.get("Unit_bedrooms", [])
                    if not isinstance(unit_bedrooms, list):
                        unit_bedrooms = [str(unit_bedrooms)] if unit_bedrooms else []
                    unit_type = item.get("Unit_type", "")
                    parking_spaces = item.get("Parking_spaces", 0)
                    formatted = f'{",".join(unit_bedrooms)},{unit_type}-{parking_spaces}'
                    parking_values.append(formatted)
            cleaned_parking = clean_number_of_parkings(parking_values)
            return {"number_of_parking": cleaned_parking}
        else:
            print(f"Error fetching parkings for project id {project_id}: {response.status_code}")
            print("Response text:", response.text)
            return {}

    def close(self):
        """
        Closes the MongoDB connection.
        """
        self.client.close()
        print("MongoDB connection closed.")

if __name__ == "__main__":
    # Instantiate and run the parser
    parser = Parser()
    parser.start()
    parser.close()
