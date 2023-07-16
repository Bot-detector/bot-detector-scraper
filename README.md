# Scraper
| Component  | Responsibility|
| ------------- | ------------- |
| ProxyManager  | the proxy manager class is responsible to get a list of proxies |
| Manager  | the manager class is responsible to initiate the workers (each worker gets one proxy), consume from the kafka player topic assign a player to a free worker to scrape, consume from the scaper topic, posting the scraped players to the api *the last responsibility should be moved to another component |
| Worker  | The worker is responsible for the scraping and its state if an error occurs the worker must return the player to the player topic |
| Scraper  | The scraper is responsible for the player object, and must gather information with the api   |
| HighscoreApi  | responsible for calling the highscore api  |
| RuneMetricsApi  | responsible for calling the runemetrics api |