#332 CA Chelcea Claudiu-Marian
#stud.acs.upb.ro

Readme:

In the main function, we receive the input files and create the output files.
We split the orders file by length into chunks and send each chunk to a thread,
chunks equally distributed between threads (level 1 threads).

Aceste thread-uri sar pana la portiunea din fisier care le este alocata
pentru citire si citesc pe bytes toate liniile care le sunt alocate, solu
tionand in acelasi timp cazurile in care pointer-ul lor va fi la jumatatea
liniei.
Pentru fiecare comanda se creeaza task-uri ce vor fi rezolvate de thread-uri
de nivel 2. Pentru a se asigura de integritatea si unicitatea datelor,
acestea sunt validate printr-un map (Concurrent hashmap).

Thread-urile de nivel 2 citesc linie cu linie fisierul de produse (asa
este specificat in enunt) si le le dau shipped pe masura ce gasesc produse,
adaugand shipped si la comanda cand toate produsele acelei comenzi sunt
trimise.
