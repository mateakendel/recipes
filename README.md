Aplikacija za upravljanje receptima razvijena u sklopu kolegija Analitika podataka velikog obujma.

## Funkcionalnosti

- Registracija i prijava korisnika
- Dodavanje i pregled recepata
- Full-text pretraživanje recepata (Elasticsearch)
- Ocjenjivanje recepata
- Praćenje korisničkih aktivnosti putem Kafka streaminga
- Keširanje pregleda recepata pomoću Redisa
- Sustav preporuke recepata temeljen na Naive Bayes modelu

## Korištene tehnologije

- Flask
- MongoDB Replica Set
- Elasticsearch
- Apache Kafka
- Redis
- Scikit-learn
- Docker
- Traefik

## Pokretanje sustava

```bash
docker compose up --build
```

## Generiranje podataka

```bash
docker compose exec recepti_app python -m scripts.data_generator
```

## Treniranje modela

```bash
docker compose exec recepti_app python -m scripts.train_model
```

## Autor
Moira Grozdanić,
Matea Kenđel
