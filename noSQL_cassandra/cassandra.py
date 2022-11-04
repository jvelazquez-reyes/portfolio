from cassandra.cluster import Cluster

# Docker
# docker pull cassandra:latest
# docker run --name test-cassandra cassandra:latest -p 9042:9042

cluster = Cluster(contact_points=['192.168.99.100'], port=9042)
session = cluster.connect('temperature_by_country')

# Read
prepared_statement = session.prepare('SELECT * FROM temperature_by_country WHERE country=?')
countries_to_lookup = ['USA', 'GERMANY']

for country_name in countries_to_lookup:
    rowCountry = session.execute(prepared_statement, [country]).one()
    print(rowCountry)

# Write sync
session.execute("INSERT INTO temperature_by_country (country, city, weather_station, temperature, time_created) VALUES ('MEXICO', 'Guadalajara', 'Winter', 15.8, 650);")

# Write async
session.execute_async("INSERT INTO temperature_by_country (country, city, weather_station, temperature, time_created) VALUES ('USA', 'New York', 'Autumn', 18, 50);")