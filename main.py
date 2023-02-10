import time
from datetime import date
import asyncio
from playwright.async_api import async_playwright
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

time_execution = time.time()

# Realiza autenticacao no firebase
cred = credentials.Certificate("firebase-adminsdk.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://default-rtdb.firebaseio.com/'
})

number_products = {}
locator_products = {}

lobascz = (
    'https://www.sitemercado.com.br/lobasczatacarejo/telemaco' +
    '-borba-loja-telemaco-borba-bela-vista-do-paraiso-r-charqueada'
)

verona = (
    'https://www.sitemercado.com.br/verona/telemaco-' +
    'borba-loja-centro-centro-rua-professora-otilia-macedo-sikorski'
)

# Define os sites em sera feito scrap
dict_sites = {'lobascz': lobascz, 'verona': verona}


# Cria um dicionario dos subdepartamentos
async def do_dict_subdepartments(locator):
    # Inicializa o dicionario
    # {subdepartamento: link}
    dict_subdepartments = {}

    # Cada i representa um departamento:
    for i in range(0, await locator.count()):
        # Coleta o locator do subdepartamento
        aux_locator = locator.nth(i).locator('a')

        # Cada j representa um subdepartamento do departamento:
        for j in range(0, await aux_locator.count()):
            # Coleta o nome do subdepartamento
            subdepartment = await aux_locator.nth(j).inner_text()

            # Coleta o link do subdepartamento
            subdepartment_link = await aux_locator.nth(j).get_attribute('href')

            # Cria um novo item no dicionario
            # {subdepartamento: link}
            dict_subdepartments[subdepartment] = subdepartment_link

    return dict_subdepartments


# Realiza o scrap dos produtos
async def scrap_and_store(
            market_name,
            context,
            subdepartment_name,
            subdepartment_link,
            asyncio_semaphore
        ):

    # Tenta isso:
    try:
        # Semaphore faz o controle da quantidade simultanea de tarefas
        async with asyncio_semaphore:
            # Inicializa o dicionario de produtos
            dict_products = None

            # Abre uma nova guia e entra no subdepartamento
            tab = await context.new_page()
            await tab.goto(
                'https://www.sitemercado.com.br' + subdepartment_link
            )

            # Espera o grid dos produtos ficar disponivel
            await tab.wait_for_selector('.products-holder', state='attached')

        # gather retorna 2 valores, pois tem 2 tarefas
        # -- scrolled serve apenas para que dict_produts receba o
        # -- dicionario de produtos retornado em do_dict_products()
        scrolled, dict_products = await asyncio.gather(
            scroll_to_the_bottom(tab, subdepartment_name),
            do_dict_products(tab, subdepartment_name)
        )

        # Fecha a guia
        await tab.close()

        # Envia o dicionario de produtos para o firebase
        await send_dict_products(
            market_name,
            subdepartment_name,
            dict_products
        )

    # Se o site demorar para carregar faz isso:
    except TimeoutError:
        print(f'Site demorando para carregar: {subdepartment_name}')


# Realiza o scroll enquanto a guia permitir
# Esta funcao é uma adaptacao de um codigo disponivel em:
# https://stackoverflow.com/questions/69183922/playwright-auto-scroll-to-bottom-of-infinite-scroll-page
async def scroll_to_the_bottom(tab, subdepartment_name):
    # Inicializa a altura anterior
    previous_height = None

    # Enquanto tiver produtos para serem carregado a pagina continua descendo
    while(True):
        # Atualiza o locator de produtos e a quantidade de produtos da guia
        # cada vez que a pagina realiza um scroll
        global number_products, locator_products
        locator_products[subdepartment_name] = tab.locator(
            '.list-product-item'
        )
        number_products[subdepartment_name] = await locator_products[
            subdepartment_name
        ].count()

        # Coleta a altura atual da pagina
        current_height = await tab.evaluate(
            '(window.innerHeight + window.scrollY)'
        )

        # Pagina desce tudo que esta carregado
        await tab.mouse.wheel(0, current_height)

        # Na primeira execucao:
        if(not previous_height):
            # Altura anterior recebe o valor da altura atual
            previous_height = current_height

            # Espera 400 ms para carregar a pagina
            await asyncio.sleep(0.4)

        # Se altura atual é igual a altura anterior:
        elif(previous_height == current_height):
            # Termina a execução da função
            return True

        # Se altura atual for diferente da altura anterior:
        else:
            # Altura anterior recebe o valor da altura atual
            previous_height = current_height

            # Espera 400 ms para carregar a pagina
            await asyncio.sleep(0.4)


# Cria um dicionario dos produtos
async def do_dict_products(tab, subdepartment_name):
    # Inicializa o dicionario de produtos
    dict_products = {}

    # Coleta o locator de produtos e a quantidade de produtos da guia
    global number_products, locator_products
    locator_products[subdepartment_name] = tab.locator('.list-product-item')
    number_products[subdepartment_name] = await locator_products[
        subdepartment_name
    ].count()

    # Contador para percorrer os produtos
    i = 0

    # Cada i representa um produto:
    while(i < number_products[subdepartment_name]):
        # Coleta o locator do produto
        product = locator_products[subdepartment_name].nth(i)

        # Coleta nome do produto
        name = await product.locator('.txt-desc-product-item').inner_text()

        # Coleta o preco do produto
        cost = await product.locator('.area-bloco-preco').inner_text()

        # Elimina caracteres invalidos
        if(u'\xa0' in cost):
            cost = cost[0:cost.index(u'\xa0')-3]

        # Coleta a 'regra' do produto
        rule = await product.locator('.regra-prd').inner_text()
        rule = rule.replace(u'\xa0', u' ')

        # Coleta o link do produto
        aux_locator = product.locator('.txt-desc-product-item')
        link = await aux_locator.locator(
            '.list-product-link'
        ).get_attribute('href')

        # Coleta o link da imagem do produto
        img_link = await product.locator('.img-fluid').get_attribute('src')

        # Cria um novo item no dicionario
        # {produto[i]: [preco, img_link, link, nome, regra]}
        dict_products[i] = {
            "cost": cost,
            "img_link": img_link,
            "link": link,
            "name": name,
            "rule": rule
        }

        # Incrementa em um o contador que percorre os produtos
        i += 1

    return dict_products


async def send_dict_products(
            market_name,
            subdepartment_name,
            dict_products
        ):

    # Envia a lista de produtos para o firebase
    ref = db.reference(f'/{market_name}%20{str(date.today())}/')
    ref.update({subdepartment_name: dict_products})


# Main
async def main():
    async with async_playwright() as p:
        # Abre o navegador e maximiza ele
        browser = await p.chromium.launch(
            headless=False,
            args=['--start-maximized']
        )
        context = await browser.new_context(no_viewport=True)

        # Para cada mercado é realizado os seguintes passos:
        for market in dict_sites:
            # Abre uma nova guia e entra o site do mercado
            page = await context.new_page()
            await page.goto(dict_sites[market])

            # Espera a lista de subdepartamentos ficar disponivel
            await page.wait_for_selector('.sub-child', state='attached')

            # Coleta o dicionario de todos os subdepartamentos
            # {subdepartamento: link}
            departments_locator = page.locator('.sub-child')
            dict_subdepartments = await do_dict_subdepartments(
                departments_locator
            )

            # Fecha a guia
            await page.close()

            # Semaphore faz o controle da quantidade simultanea de tarefas
            # realizadas pelo gather (neste caso limitado a 1)
            asyncio_semaphore = asyncio.BoundedSemaphore(1)

            # Cria uma lista que contera todas as minhas tarefas de scrap
            # Este 'for' é uma adaptacao de um codigo disponivel em:
            # https://www.ti-enxame.com/pt/python/e-possivel-limitar-o-numero-de-corotinas-em-execucao-de-forma-simultanea-no-assincio/838558688/
            tasks = []
            for subdepartment in dict_subdepartments:
                tasks.append(asyncio.ensure_future(
                    scrap_and_store(
                        market,
                        context,
                        subdepartment,
                        dict_subdepartments[subdepartment],
                        asyncio_semaphore
                    )
                ))

            # Realiza as tarefas
            await asyncio.gather(*tasks)

        # Fecha o navegador
        await browser.close()


# Inicializa o código
asyncio.run(main())

# Monitora o tempo de execucao do codigo
time_execution = time.time() - time_execution
min = int(time_execution//60)
seg = time_execution - int(time_execution//60)*60
print(f'Tempo de execucao: {min} min e {seg:.2f} s')
