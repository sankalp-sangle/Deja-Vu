'''
This file defines forms used in app.py For details on how forms
are used in Flask, refer to:
https://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-iii-web-forms
'''

from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SubmitField
from wtforms.validators import DataRequired

class QueryForm(FlaskForm):
    time = StringField('Enter time column', validators=[DataRequired()])
    value = StringField('Enter value', validators=[DataRequired()])
    metric = StringField('Enter metric', validators=[DataRequired()])

    submit1 = SubmitField('Submit Query')

class PacketSearchForm(FlaskForm):
    hash = StringField('Enter packet hash', validators=[DataRequired()])
    submit = SubmitField('View details')

class RandomQuery(FlaskForm):
    query = StringField('Enter a general query:', validators=[DataRequired()])

    submit2 = SubmitField('Submit Query')

class SimpleButton(FlaskForm):
    submit = SubmitField('Plot in Grafana')