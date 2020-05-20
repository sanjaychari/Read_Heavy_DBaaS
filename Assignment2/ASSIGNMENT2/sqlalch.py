from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
#from flask_sqlalchemy import text

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///assignment2.db'
#app.config['SQLALCHEMY_DATABASE_URI'] 	= 'sqlite:////home/ubuntu/CC_Assignment1/test.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class User(db.Model):
    UserId = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(), unique=True, nullable=False)
    password = db.Column(db.String(), unique=False, nullable=False)

    def __repr__(self):
        return self.username

class Ride(db.Model):
	rideId = db.Column(db.Integer, primary_key=True)
	Created_by = db.Column(db.String(), unique=False, nullable=False)
	Timestamp = db.Column(db.TIMESTAMP, primary_key=False, nullable=False)
	source = db.Column(db.Integer, unique=False, nullable=False)
	destination = db.Column(db.Integer, unique=False, nullable=False)
	users = db.Column(db.String(),unique=False,nullable=False)

	def __repr__(self):
		return self.source+" "+self.destination

db.init_app(app)
db.create_all()

