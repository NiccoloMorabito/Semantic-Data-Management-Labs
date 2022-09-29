import pandas as pd
from random import randrange, sample, choice

def join_journal_volume_paper():
    a = pd.read_csv("paper_appears_volume.csv")
    b = pd.read_csv("volume_ispartof_journal.csv")

    merged = a.merge(b, on="idvolume")
    merged.drop("idvolume", axis=1, inplace=True)
    merged.to_csv("paper_publishedin_journal.csv", index=False)

def join_conference_edition_paper():
    a = pd.read_csv("edition_includes_paper.csv")
    b = pd.read_csv("conference_composed_edition.csv")

    merged = a.merge(b, on="idedition")
    merged.drop("idedition", axis=1, inplace=True)
    merged.to_csv("paper_publishedin_conference.csv", index=False)

def generate_conference_areas():
    conference_ids = pd.read_csv("conference.csv")["id"]
    area_ids = set(pd.read_csv("area.csv")["id"])

    generated_data = list()

    for conf_id in conference_ids:
        # choose randomly n elements from area_ids, where n is a random number between 1 and 10
        conf_areas = sample(area_ids, randrange(1, 10))
        generated_data += [(conf_id, conf_area) for conf_area in conf_areas]
    
    generated_df = pd.DataFrame(generated_data, columns = ["conferenceid", "areaid"])
    generated_df.to_csv("conference_focuseson_area.csv", index=False)

def generate_venuehead_handles():
    venuehead_ids = pd.read_csv("venuehead.csv")["id"]

    handles_journal = list()
    journal_ids = pd.read_csv("journal.csv")["id"]
    for journal_id in journal_ids:
        venuehead_id = choice(venuehead_ids)
        handles_journal.append((venuehead_id, journal_id))
    handles_journal_df = pd.DataFrame(handles_journal, columns = ["venueheadid", "journalid"])
    handles_journal_df.to_csv("venuehead_handles_journal.csv", index=False)
    del handles_journal, journal_ids, handles_journal_df, venuehead_id, journal_id
    
    handles_conference = list()
    conference_ids = pd.read_csv("conference.csv")["id"]
    for conference_id in conference_ids:
        venuehead_id = choice(venuehead_ids)
        handles_conference.append((venuehead_id, conference_id))
    handles_conference_df = pd.DataFrame(handles_conference, columns = ["venueheadid", "conferenceid"])
    handles_conference_df.to_csv("venuehead_handles_conference.csv", index=False)

def generate_venuehead_assigns():
    venuehead_ids = pd.read_csv("venuehead.csv")["id"]

    reviews = pd.read_csv("review.csv")
    review_ids = reviews["authorid"] + "@" + reviews["paperid"]
    
    assigns_review = list()
    for review_id in review_ids:
        venuehead_id = choice(venuehead_ids)
        assigns_review.append((venuehead_id, review_id))
    assigns_review_df = pd.DataFrame(assigns_review, columns = ["venueheadid", "reviewid"])
    assigns_review_df.to_csv("venuehead_assigns_review.csv", index=False)
    

if __name__ == '__main__':
    generate_venuehead_assigns()
