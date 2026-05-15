#!/bin/bash

# Script per l'esecuzione automatizzata dei job Spark su EC2
# Esegue tutte le combinazioni possibili di Query e Backend

# 1. Definizione dei set di dati da processare
QUERIES=("monthly_performance" "arrival_delay_ranking" "hourly_delay_percentiles")  # Aggiungi o rimuovi le query del progetto
BACKENDS=("rdd" "dataframe" "sql")                                                  # I due approcci richiesti
OUTPUT_TYPE="postgres"                                                              # Output predefinito (es. hdfs o redis)

echo "[SPARK-BENCHMARK] Inizio esecuzione batch dei job Spark..."
echo "--------------------------------------------------------"

# 2. Cicli annidati per ciclare su ogni combinazione
for QUERY_TYPE in "${QUERIES[@]}"; do
    for BACKEND_TYPE in "${BACKENDS[@]}"; do

        echo ""
        echo "========================================================"
        echo "[SPARK] Avvio nuovo Job"
        echo "========================================================"

        # Esecuzione del comando dentro il container spark-master
        ./spark-submit.sh $QUERY_TYPE $BACKEND_TYPE $OUTPUT_TYPE

        # Controllo dello stato di uscita del job precedente
        if [ $? -eq 0 ]; then
            echo "[SUCCESS] Job completato con successo: $QUERY_TYPE ($BACKEND_TYPE)"
        else
            echo "[ERROR] Il Job ha riscontrato un fallimento: $QUERY_TYPE ($BACKEND_TYPE)"
            # Opzionale: togli il commento qui sotto se vuoi bloccare tutto lo script in caso di errore di un singolo job
            exit 1
        fi

        echo "--------------------------------------------------------"
        # Un piccolo sleep per far respirare il cluster e dare tempo a Redis/HDFS di liberare le risorse
        sleep 3

    done
done

echo "[SPARK-BENCHMARK] Tutti i job sono stati inviati al cluster."