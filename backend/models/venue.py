from pydantic import BaseModel


class Venue(BaseModel):
     

    title: str
    category: str
    date: str
    location: str
    url:str
  