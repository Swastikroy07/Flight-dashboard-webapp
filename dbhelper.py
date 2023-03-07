import mysql.connector

class DB:
    def __init__(self):
        # connect to database 
        try:
            self.conn = mysql.connector.connect(
                host="127.0.0.1",
                user="swastik",
                password="12345",
                database="flights"
            )
            self.mycursor=self.conn.cursor()
            print("connection established")
        except:
            print("connection error ")

    def fetch_city_name(self):
        city=[]
        self.mycursor.execute("""
        SELECT DISTINCT(Destination) FROM flights.flights
        UNION 
        SELECT DISTINCT(source) FROM flights.flights
        """)
        data = self.mycursor.fetchall()
        print(data)

        for item in data :
            city.append(item[0])
        return city

    def fetch_all_flights(self,souce,destination):
        self.mycursor.execute(f"""
            SELECT Airline,Route,Dep_Time,Price FROM flights
            WHERE Source ="{souce}" AND Destination ="{destination}"
        """)

        data = self.mycursor.fetchall()

        return data
    
    def fetch_airline_frequency(self):
        airline=[]
        frequency=[]
        self.mycursor.execute("""
            SELECT Airline,COUNT(*) FROM flights
            GROUP BY Airline
        """)

        data = self.mycursor.fetchall()
        for item in data:
            airline.append(item[0])
            frequency.append(item[1])

        return airline,frequency
    
    def busy_airport(self):
        city=[]
        frequency=[]
        self.mycursor.execute("""
            SELECT Source,COUNT(*) FROM (SELECT Source FROM flights
										UNION ALL
										SELECT Destination FROM flights) t
            GROUP BY t.Source 
            ORDER BY COUNT(*) DESC
        """)
        data = self.mycursor.fetchall()
        for item in data:
            city.append(item[0])
            frequency.append(item[1])

        return city,frequency
    
    def daily_frequency(self):
        date=[]
        frequency=[]
        self.mycursor.execute("""
            SELECT Date_of_Journey,COUNT(*) FROM flights
            GROUP BY Date_of_Journey
        """)

        data = self.mycursor.fetchall()
        for item in data:
            date.append(item[0])
            frequency.append(item[1])

        return date,frequency
