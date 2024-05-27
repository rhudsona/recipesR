# pacman é uma biblioteca de gerenciamento de pacotes. A biblioteca agiliza o fluxo de trabalho, reduzindo o tempo de recuperação de funções, reduzindo o código e 
# integrando a funcionalidade das funções básicas para executar várias ações simultaneamente, carregando e se necessário instalando os pacotes ausentes.
# Os nomes das funções no pacote pacman seguem o formato p_xxx onde 'xxx' é a tarefa que a função executa.


p_install(rpart,MASS)                    # Instala os pacotes rpart e MASS
p_load(rpart,MASS)                       # Carrega e INSTALA os pacotes rpart e MASS
p_install_gh("hadley/httr@v0.4")         # Instala pacotes do github.  Os pacotes são passados ​​como endereços de repositório de 
                                         # vetores de caracteres no formato username/repo[/subdir][@ref|#pull].
p_install_version()                      # Instale a versão mínima dos pacotes
p_load_gh()                              # Carrega e Instala os pacotes do github
p_unload()                               # Descarrega pacotes do caminho de pesquisa, equivalente ao detach()
p_update()                               # Atualiza pacotes desatualizados
