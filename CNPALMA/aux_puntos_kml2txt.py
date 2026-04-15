
import xml.etree.ElementTree as ET

tree = ET.parse("puntos.kml")
root = tree.getroot()

ns = {"kml": "http://www.opengis.net/kml/2.2"}

with open("puntos.txt", "w") as f:
    for placemark in root.findall(".//kml:Placemark", ns):
        name = placemark.find("kml:name", ns).text
        coords = placemark.find(".//kml:coordinates", ns).text.strip()
        lon, lat, *_ = coords.split(",")
        f.write(f"{name} {lon} {lat}\n")
