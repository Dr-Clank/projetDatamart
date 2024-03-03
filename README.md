# lieu des fichiers

Veuillez placer les fichiers Json et CSV dans le dossier 'projetDatamart/ProjetETL/files'

# projetDatamart

export PATH=/usr/gide/jdk-1.8/bin:$PATH
export PATH=/usr/gide/sbt-1.3.13/bin:$PATH

sbt -J-Xmx16G run

# connexion

ssh [login]@eluard

. /opt/oraenv.sh
sqlplus [login] 
$mdp a entrer$ [login]

# fichier SQL a executer

!!Attention exécuter cette étape après avoir lancer le programme scala!!
Executer requeteSQL.sql sur la base stendhal pour avoir les vues et les vue matérialisé
