# AMQ Java
Proeto criado com o intuito de estudar as seguintes tecnologias:
* RabbitMQ
* Python

Possíveis cenários

1. RPC (com terceirização escalável do processamento):
    1. API sobe serviço criando fila exclusiva no AMQ
    2. API recebe requisição e publica dados para processamento em fila própria no AMQ. Inclui propriedade referenciando fila onde resposta deve ser publicada
    3. API recebe resposta do AMQ e devolve para o client.

    >  Vantagens:
    >> Utilizar _durable queue_ executa persistência de dados no disco
    >
    >> Executores podem ser configurados para receber apenas um processamento por vez
    >
    >> Possibilidade de escalar os executores dos cálculos em servidores diferentes

Links úteis:
> * [URL Local](http://localhost:15672)
> * [Tutorial seguido](https://www.rabbitmq.com/tutorials/tutorial-six-java.html)