from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField, SubmitField
from wtforms.validators import DataRequired

class SampleForm(FlaskForm):
    value = StringField('Enter value to plot', validators=[DataRequired()])
    submit = SubmitField('Submit Query')

class PacketSearchForm(FlaskForm):
    hash = StringField('Enter packet hash', validators=[DataRequired()])
    submit = SubmitField('View details')

class SimpleButton(FlaskForm):
    submit = SubmitField('Plot in Grafana', validators=[DataRequired()])