
import json
import xml.etree.ElementTree as ET

def parse_orl_xml(xml_string):
    try:
        # Parse the XML string
        root = ET.fromstring(xml_string)
        
        # Extract data into a dictionary
        data = {
            "Action": root.attrib.get("Action", ""),
            "Payment": root.attrib.get("Payment", ""),
            "Balance": root.attrib.get("Balance", ""),
            "Total": root.attrib.get("Total", ""),
            "Date": root.attrib.get("Date", ""),
            "CheckinDate": root.attrib.get("CheckinDate", ""),
            "CheckoutDate": root.attrib.get("CheckoutDate", "")
        }
        
        space = root.find(".//Space")
        if space is not None:
            data.update({
                "Space_ID": space.attrib.get("ID", ""),
                "Space_Num": space.attrib.get("Num", ""),
                "Space_Type": space.attrib.get("Type", ""),
                "Space_UOM": space.attrib.get("UOM", ""),
                "Space_FromDate": space.attrib.get("FromDate", ""),
                "Space_ToDate": space.attrib.get("ToDate", ""),
                "Space_Amount": space.attrib.get("Amount", "")
            })
            
        consumer = root.find(".//Consumer")
        if consumer is not None:
            data.update({
                "Consumer_ID": consumer.attrib.get("ID", ""),
                "FirstName": consumer.findtext("FirstName", ""),
                "LastName": consumer.findtext("LastName", ""),
                "Address1": consumer.findtext("Address1", ""),
                "Address2": consumer.findtext("Address2", ""),
                "City": consumer.findtext("City", ""),
                "State": consumer.findtext("State", ""),
                "ZipCode": consumer.findtext("ZipCode", ""),
                "Country": consumer.findtext("Country", ""),
                "Phone": consumer.findtext("Phone", ""),
                "CellPhone": consumer.findtext("CellPhone", ""),
                "Fax": consumer.findtext("Fax", ""),
                "Email": consumer.findtext("Email", "")
            })

        unit = root.find(".//Unit")
        if unit is not None:
            data.update({
                "UnitType": unit.findtext("UnitType", ""),
                "UnitLength": unit.findtext("UnitLengh", ""),
                "UnitWidth": unit.findtext("UnitWidth", ""),
                "AmpService": unit.findtext("AmpService", "")
            })

        payments = root.find(".//Payments/Payment")
        if payments is not None:
            data.update({
                "Payment_Type": payments.attrib.get("Type", ""),
                "Payment_Amount": payments.attrib.get("Amount", ""),
                "Payment_CardNumber": payments.attrib.get("CardNumber", ""),
                "Payment_Date": payments.attrib.get("Date", "")
            })

        return data

    except ET.ParseError as e:
        print(f"Failed to parse XML: {e}")
        return None

# Read the JSON lines from the file
file_path = r'C:\gulp-projects\leisure-4037\dboORDER_LOGS.json'
updated_json_data = []
import time
i = 0
with open(file_path, 'r', encoding='utf-8') as file:
    for line in file:
        if line[0]!='{':
            print("Skipped ", i+1)
            print(line)
            time.sleep(5)
            continue
        # time.sleep(10)
        i+=1
        print(line)

        # Parse each JSON object
        json_data = json.loads(line)
        orl_xml = json_data.get("ORL_XML", "")
        # Parse the XML data within each JSON object
        parsed_data = parse_orl_xml(orl_xml)
        print(parsed_data)
        if parsed_data:
            # Replace the ORL_XML field with the parsed data
            json_data["ORL_XML"] = parsed_data
        updated_json_data.append(json_data)

# Write the updated JSON data back to the file or a new file
output_file_path = 'updated_file1.json'
with open(output_file_path, 'w') as outfile:
    for updated_json in updated_json_data:
        json.dump(updated_json, outfile)
        outfile.write('\n')

print(f"Updated JSON data has been written to {output_file_path}")
