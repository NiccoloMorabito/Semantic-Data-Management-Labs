import pandas as pd

# Helpers
def create_dummy_edge( pd_container, pd_contained, container_name, contained_name, n_repeats=15, random_state=64 ):
    pd_res = pd.DataFrame()
    container_ids = pd_container.id.repeat(n_repeats).reset_index(drop=True)
    container_ids = container_ids.sample(pd_contained.shape[0], replace=True, random_state=random_state)
    pd_res['id'+contained_name] = pd_contained.id
    pd_res['id'+container_name] = list(container_ids)
    return pd_res

# 1. Load the Data

# Nodes
pd_author = pd.read_csv(r'../Data/Source/authors.csv')
pd_topic = pd.read_csv(r'../Data/Source/keywords.csv')
pd_paper = pd.read_csv(r'../Data/Source/papers.csv')
pd_year = pd.read_csv(r'../Data/Source/years.csv')
pd_conference = pd.read_csv(r'../Data/Dummy/conference.csv')
pd_edition = pd.read_csv(r'../Data/Dummy/edition.csv')
pd_journal = pd.read_csv(r'../Data/Dummy/journal.csv')
pd_volume = pd.read_csv(r'../Data/Dummy/volume.csv')
pd_review = pd.read_csv(r'../Data/Dummy/reviews.csv')
pd_university = pd.read_csv(r'../Data/Dummy/university.csv')
pd_company = pd.read_csv(r'../Data/Dummy/company.csv')


# Edges
pd_paper_about_topic = pd.read_csv(r'../Data/Source/has_keyword.csv')
pd_paper_cites_paper = pd.read_csv(r'../Data/Source/cites.csv')
pd_author_writes_paper = pd.read_csv(r'../Data/Source/wrote.csv')
pd_author_reviews_paper = pd.read_csv(r'../Data/Source/reviewed.csv')

# Other
pd_author_of_paper = pd.read_csv(r'../Data/Source/corresponding.csv')

# 2. Clean and keep whats needed

# Filtering
pd_author = pd_author[['_id', 'name', 'email']]
pd_paper = pd_paper[['_id', 'title', 'year', 'lang', 'isbn', 'abstract']]
pd_paper['abstract'] = pd_paper.abstract.str.replace('[^\w\s]', '')
pd_paper['title'] = pd_paper.title.str.replace('[^\w\s]', '')

# Transforming
pd_author_reviews_paper['review'] = pd_review.review.sample(pd_author_reviews_paper.shape[0], replace=True, random_state=64).reset_index(drop=True)
pd_author_reviews_paper['timestamp'] = pd_review.timestamp.sample(pd_author_reviews_paper.shape[0], replace=True, random_state=64).reset_index(drop=True)
pd_author_reviews_paper['decision'] = pd.Series([0, 1]).sample(pd_author_reviews_paper.shape[0], replace=True, random_state=64).reset_index(drop=True)

pd_conference['numreviewers'] = pd.Series([2,3,4,5,6,7]).sample(pd_conference.shape[0], replace=True, random_state=64).reset_index(drop=True)
pd_journal['numreviewers'] = pd.Series([2,3,4,5,6,7]).sample(pd_journal.shape[0], replace=True, random_state=64).reset_index(drop=True)


# Renaming
pd_author.columns = ['id','name','email']
pd_topic.columns = ['id','topic']
pd_paper_about_topic.columns = ['paperid', 'topicid']
pd_paper_cites_paper.columns = ['paperidsrc', 'paperidref']
pd_paper = pd_paper.rename(columns={'_id': 'id'})
pd_year = pd_year.rename(columns={'_id': 'id'})
pd_conference = pd_conference.rename(columns={'_id': 'id'})
pd_edition = pd_edition.rename(columns={'_id': 'id'})
pd_journal = pd_journal.rename(columns={'_id': 'id'})
pd_volume = pd_volume.rename(columns={'_id': 'id'})

# 3. Generating dummy edges
pd_conference_composed_edition = create_dummy_edge(pd_conference, pd_edition, 'conference', 'edition')
pd_edition_includes_paper = create_dummy_edge(pd_edition, pd_paper, 'edition', 'paper')
pd_volume_ispartof_journal = create_dummy_edge(pd_volume, pd_journal, 'volume', 'journal')
pd_paper_appears_volume = create_dummy_edge(pd_volume, pd_paper, 'volume', 'paper')
pd_author_affiliatedto_company = create_dummy_edge(pd_company, pd_author, 'company', 'author')
pd_author_affiliatedto_university = create_dummy_edge(pd_university, pd_author, 'university', 'author')
#pd_edition_years

# 4. Export the data
pd_author.to_csv( '../Data/Wrangled/author.csv', index=False )
pd_author_of_paper.to_csv( '../Data/Wrangled/author_of_paper.csv', index=False )
pd_author_reviews_paper.to_csv( '../Data/Wrangled/author_reviews_paper.csv', index=False )
pd_author_writes_paper.to_csv( '../Data/Wrangled/author_writes_paper.csv', index=False )
pd_conference.to_csv( '../Data/Wrangled/conference.csv', index=False )
pd_conference_composed_edition.to_csv( '../Data/Wrangled/conference_composed_edition.csv', index=False )
pd_edition.to_csv( '../Data/Wrangled/edition.csv', index=False )
pd_edition_includes_paper.to_csv( '../Data/Wrangled/edition_includes_paper.csv', index=False )
pd_journal.to_csv( '../Data/Wrangled/journal.csv', index=False )
pd_paper.to_csv( '../Data/Wrangled/paper.csv', index=False )
pd_paper_about_topic.to_csv( '../Data/Wrangled/paper_about_topic.csv', index=False )
pd_paper_appears_volume.to_csv( '../Data/Wrangled/paper_appears_volume.csv', index=False )
pd_paper_cites_paper.to_csv( '../Data/Wrangled/paper_cites_paper.csv', index=False )
pd_topic.to_csv( '../Data/Wrangled/topic.csv', index=False )
pd_volume.to_csv( '../Data/Wrangled/volume.csv', index=False )
pd_volume_ispartof_journal.to_csv( '../Data/Wrangled/volume_ispartof_journal.csv', index=False )
pd_year.to_csv( '../Data/Wrangled/year.csv', index=False )
pd_company.to_csv( '../Data/Wrangled/company.csv', index=False )
pd_university.to_csv( '../Data/Wrangled/university.csv', index=False )
pd_author_affiliatedto_company.to_csv( '../Data/Wrangled/author_affiliatedto_company.csv', index=False )
pd_author_affiliatedto_university.to_csv( '../Data/Wrangled/author_affiliatedto_university.csv', index=False )