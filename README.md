# recommender_arena

## How to use:

#### build model

rebuild=Rebuild_Model()

#### get perfomance

perfomance=rebuild.get_model_perfomance()

`>>> perfomance`
0.96691173

#### update model while running on production

update=Rebuild_Model(online=True)

#### get perfomance

perfomance=update.get_model_perfomance()

`>>> perfomance`
0.95891479

#### get recommended item

hireko = Hireko()
rek_exist_item,rek_new_item = hireko.predict('12087','',7) #format('user_id','feature',number of items)

```
>>> rek_exist_item
[{'item_id': '5505', 'score': -3.990360736846924},
 {'item_id': '347', 'score': -4.102499485015869},
 {'item_id': '5500', 'score': -4.125725269317627},
 {'item_id': '5554', 'score': -4.430047035217285},
 {'item_id': '5551', 'score': -4.430047035217285}]
```
```>>> rek_new_item
[]
```
