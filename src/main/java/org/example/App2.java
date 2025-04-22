package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Ventes par Ville et Année").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lire le fichier
        JavaRDD<String> rddLines = sc.textFile("ventes.txt");

        // Enlever l'entête
        String header = rddLines.first();
        JavaRDD<String> rddSansEntete = rddLines.filter(line -> !line.equals(header));

        // Extraire (ville, année) comme clé, et le prix comme valeur
        JavaPairRDD<Tuple2<String, String>, Double> ventesParVilleEtAnnee = rddSansEntete.mapToPair(line -> {
            String[] fields = line.split(",");
            String date = fields[0];
            String ville = fields[1];
            double prix = Double.parseDouble(fields[3]);
            String annee = date.split("-")[0]; // Extraire l'année
            return new Tuple2<>(new Tuple2<>(ville, annee), prix);
        });

        // Calculer le total par (ville, année)
        JavaPairRDD<Tuple2<String, String>, Double> totalParVilleEtAnnee = ventesParVilleEtAnnee.reduceByKey(Double::sum);

        // Afficher le résultat
        totalParVilleEtAnnee.foreach(tuple -> {
            Tuple2<String, String> key = tuple._1;
            Double total = tuple._2;
            System.out.println("Ville : " + key._1 + " | Année : " + key._2 + " | Total des ventes : " + total);
        });

    }
}
