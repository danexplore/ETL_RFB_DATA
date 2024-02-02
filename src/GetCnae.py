import os
import PySimpleGUI as sg
import pandas as pd
import numpy as np


def Cnae_List():
    """
    Retorna a lista de CNAES segundo o arquivo csv disponibilizado pela receita.
    """

    diretorio_atual = os.getcwd()
    arquivo_cnae = os.path.join(diretorio_atual, 'Dados_RFB', 'Cnae.csv')
    
    # Leitura do arquivo de cnaes usando Pandas
    cnaes_df = pd.read_csv(arquivo_cnae, sep=';', header=None)
    matriz_cnae = cnaes_df.values.astype(np.str_)
    
    # Transforma NDArray[str_] em list
    cnaes_list = [cnae for cnae in matriz_cnae]
    return cnaes_list


def Selecionar_CNAE():
    """
    Cria uma caixa de seleção do CNAE principal para filtrar os estabelecimentos.
    Atualiza ao clicar no botão 'Pesquisar'
    """
    cnae_list = Cnae_List()
    
    layout = [
    [sg.Text("Pesquisar CNAE:"), sg.InputText(key="-PESQUISAR-", size=(20, 1)), sg.Button("Pesquisar")],
    [sg.Listbox(values=[], size=(65, 15), key="-CNAE_LIST-", enable_events=True, select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE)],
    [sg.Text('CNAES'), sg.Listbox(values=[], key='-TEXTO-', size=(40,10))],
    [sg.Button("Adicionar Cnaes"), sg.Button('Limpar')],
    [sg.Button("Salvar e Finalizar")]
    ]

    window = sg.Window("Pesquisa de CNAEs", layout, size=(800, 600))    

    cnaes_selecionados = []
    cnaes_selecionados = []

    while True:
        event, values = window.read()

        # Finaliza o 'while' e fecha a janela.
        if event == sg.WINDOW_CLOSED or event == "Salvar e Finalizar":
            break
        
        # Pesquisa se há cnae com o valor inserido
        elif event == "Pesquisar":
            valor_pesquisa = values["-PESQUISAR-"]
            cnae_encontrado = [(cnae, desc) for cnae, desc in cnae_list if valor_pesquisa in cnae or valor_pesquisa in str(desc)]
            update_text = []

            for cnae, desc in cnae_encontrado:
                text = f'{cnae} - {desc}'
                update_text.append(text)
            window["-CNAE_LIST-"].update(values=update_text)
        
        # Inseri os cnaes selecionados na lista de retorno.
        elif event == "Adicionar Cnaes":
            selected_value = [values['-CNAE_LIST-'] if values['-CNAE_LIST-'] else None]
            window['-CNAE_LIST-'].update(set_to_index=-1)

            for cnae_desc in selected_value:
                selected_cnae = []
                selected_cnae.clear()
                selected_cnae.append(cnae_desc)
    
            if len(selected_cnae) > 0:
                for cnae in selected_cnae[0]:
                    cnaes_selecionados.append(cnae)
                window['-TEXTO-'].update(values=cnaes_selecionados)
        
        # Limpa os cnaes selecionados
        elif event == 'Limpar':
            cnaes_selecionados.clear()
            window['-TEXTO-'].update(values=cnaes_selecionados)
        
    window.close()
    return cnaes_selecionados

def get_cnaes_number():
    cnaes = Selecionar_CNAE()
    lista_numeros_cnae = [int(numero.split(' - ')[0]) for numero in cnaes]
    return lista_numeros_cnae

def main():
    return get_cnaes_number()

if __name__ == '__main__':
    main()
