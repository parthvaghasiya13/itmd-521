sqoop import --connect jdbc:mysql://localhost/itmd521 --username root -P --query "Select * from records where temp BETWEEN 380 AND 420 and \$CONDITIONS" --target-dir /user/vagrant/import_Parth -m 1
