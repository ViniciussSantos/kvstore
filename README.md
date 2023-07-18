# kvstore

assuma que vamos fazer input de uma chave feijao

1 - cliente 1 PUT feijao value
2 - O servidor responde PUT_OK com timestamp com um valor arbitrário 5
3 - inserimos isso no map das escritas recentes do cliente 1
4 - outro cliente 2 faz um put da mesma chave
5 - Esse novo PUT atualiza o timestamp de 5 para 6, por exemplo
6 - assuma que esta acontecendo um delay entre as replicaçôes,
ou seja, apenas o lider tem a key feijao com o timestamp 6
7-  cliente 1 envia um GET ao lider e atualiza seu timestamp para 6
8 - logo em seguida, o cliente 1 faz um outro GET a um dos servidores com timestamp 5
9 - O timestamp desse servidor que ainda não recebeu replicação será 5
10 -O timestamp do servidor (=5) é menor que o timestamp do cliente 1(=6) (5 < 6)
11 - TRY OTHER SERVER OR LATER

talvez seja interessante fazer o GET do passo 7 e 8 usando o cliente 2, já que pode haver momentos onde o cliente 1 está esperando um PUT_OK'