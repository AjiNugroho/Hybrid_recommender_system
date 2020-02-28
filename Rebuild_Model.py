import numpy as np
from lightfm import LightFM
from lightfm.data import Dataset
from lightfm.evaluation import auc_score
from lightfm.evaluation import precision_at_k
from scipy.sparse import coo_matrix
import scipy.sparse as sp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit, sum, coalesce, count, regexp_replace, split, explode
from lightfm import cross_validation
import scipy.sparse as sp
import dill

class Rebuild_Model(object):
    '''
    Kelas ini berfungsi untuk membuat ulang model pada akhir hari
    semua user baru dan item baru akan di tambahkan dan akan di training ulang
    kelas ini juga berfungsi untuk membangun model pada proses awal training
    '''
    def __init__(self, spark_session, parquet_dir, learning_schedule='adagrad',loss='warp',learning_rate=0.05,
                item_alpha=0.0,user_alpha=0.0,online = False,new_item_exist= False):
        
        assert item_alpha >= 0.0
        assert user_alpha >= 0.0
        assert learning_schedule in ('adagrad', 'adadelta')
        assert loss in ('logistic', 'warp', 'bpr', 'warp-kos')
        
        self.spark = spark_session
        self.parquet_dir = parquet_dir
        self.item_alpha = item_alpha
        self.user_alpha = user_alpha
        self.learning_schedule = learning_schedule
        self.loss = loss
        self.learning_rate = learning_rate

        self.df_user,self.df_item,self.df_interaction,self.df_user_feature,self.df_item_feature,self.user_list_feature,self.item_list_feature = self.load_file()

        #mapping
        self.user_to_index,self.index_to_user = self.mapping_index([x["user_id"] for x in self.df_user.collect()])
        self.item_to_index,self.index_to_item = self.mapping_index([x["item_id"] for x in self.df_item.collect()])
        self.ufeat_to_index,self.index_to_ufeat = self.mapping_index(self.user_list_feature)
        self.ifeat_to_index,self.index_to_ifeat = self.mapping_index(self.item_list_feature)

        #interaction
        self.mat_interaction = self.get_interaction_matrix(self.df_interaction,'user_id','item_id','rate',self.user_to_index,self.item_to_index)

        self.mat_user_feature = self.get_interaction_matrix(self.df_user_feature,'user_id','feature','weight',self.user_to_index,self.ufeat_to_index)

        self.mat_item_feature = self.get_interaction_matrix(self.df_item_feature,'item_id','feature','weight',self.item_to_index,self.ifeat_to_index)

        #jika sedang online(hanya update)
        
        if online:
            self.model_ = self.update(self.mat_interaction,self.mat_user_feature,self.mat_item_feature)
        else:
            self.model_ = self.train(self.mat_interaction,self.mat_user_feature,self.mat_item_feature)

        #print(self.calculate_auc_score(self.model_,self.mat_interaction,self.mat_item_feature,self.mat_user_feature))
        
        #saving
        self.save_model()
        self.save_matrix()
        self.save_mapping()
        
        if new_item_exist:
            self.save_new_matrix()


    def load_file(self):
        df_item = self.spark.read.csv(self.parquet_dir + "items", header=True)
        df_user = self.spark.read.csv(self.parquet_dir + "users", header=True)
        df_interaction = self.spark.read.csv(self.parquet_dir + "interactions", header=True)
        df_user_feature= self.spark.read.csv(self.parquet_dir + 'user_features', header=True)
        df_item_feature= self.spark.read.csv(self.parquet_dir + 'item_features', header=True)

        #cleansing data
        df_user = df_user.fillna({'category_subscribe':'No_Categories','subscribe':'No_Subscribe'})
        df_item = df_item.withColumn("genre_name", regexp_replace("genre_name", r",", r""))

        #create feature_list
        user_feat2 = np.array(np.unique(df_user.select("gender").collect()))
        user_feat3 = np.array(np.unique(df_user.select(explode(split("category_subscribe", ","))).collect()))
        user_feat4 = np.array(np.unique(df_user.select(explode(split("subscribe", ","))).collect()))
        user_list_feature = np.concatenate((user_feat2,user_feat3,user_feat4),axis=0)

        item_feat1 = np.array(np.unique(df_item.select("cat_name").collect()))
        item_feat2 = np.array(np.unique(df_item.select("genre_name").collect()))
        item_feat3 = np.array(np.unique(df_item.select("eo_id").collect()))
        item_list_feature = np.concatenate((item_feat1,item_feat2,item_feat3), axis=0)

        #create feature df
        df_item_feature = df_item_feature.withColumn("feature", regexp_replace("feature", r",", r""))
        user_list = [x["user_id"] for x in df_user.collect()]
        df_user_feature = df_user_feature.where(col("user_id").isin(user_list))

        item_list = [x["item_id"] for x in df_item.collect()]
        df_item_feature = df_item_feature.where(col("item_id").isin(item_list))

        return df_user,df_item,df_interaction,df_user_feature,df_item_feature,user_list_feature,item_list_feature
    
    
    def mapping_index(self,list_i):
        idx_to_map={}
        map_to_idx={}
    
        for index,mapx in enumerate(list_i):
            map_to_idx[mapx]=index
            idx_to_map[index]=mapx
        
        return map_to_idx,idx_to_map

    def get_interaction_matrix(self,df, df_column_as_row, df_column_as_col, df_column_as_value, row_indexing_map, 
                          col_indexing_map):
    
        row = np.array([row_indexing_map[x[df_column_as_row]] for x in df.collect()]).astype(int)
        col = np.array([col_indexing_map[x[df_column_as_col]] for x in df.collect()]).astype(int)
        value = np.array([x[df_column_as_value] for x in df.collect()]).astype(int)
    
        return coo_matrix((value, (row, col)), shape = (len(row_indexing_map),len(col_indexing_map)))

    def calculate_auc_score(self,lightfm_model, interactions_matrix,item_features, user_features): 
        score = auc_score( 
            lightfm_model, interactions_matrix, 
            item_features=item_features, 
            user_features=user_features, 
            num_threads=4).mean()
        return score

    def get_model_perfomance(self):
        return (self.calculate_auc_score(self.model_,self.mat_interaction,self.mat_item_feature,self.mat_user_feature))


    def train(self,mat_interaction,mat_user_feature,mat_item_feature,epoch=20,threads=8):
        model = LightFM(loss=self.loss,learning_rate=self.learning_rate,item_alpha=self.item_alpha,user_alpha=self.user_alpha)
        model.fit(mat_interaction,
                  user_features=mat_user_feature,
                  item_features=mat_item_feature,
                  epochs=epoch,num_threads=threads,
                  verbose=False)

        return model
    
    def update(self,mat_interaction,mat_user_feature,mat_item_feature,epoch=3,threads=8):
        model = LightFM(loss=self.loss,learning_rate=self.learning_rate,item_alpha=self.item_alpha,user_alpha=self.user_alpha)
        model.fit_partial(mat_interaction,
                  user_features=mat_user_feature,
                  item_features=mat_item_feature,
                  epochs=epoch,num_threads=threads,
                  verbose=False)

        return model
    
    def save_model(self,path_save = 'model'):

        if path_save is not None:
            with open(path_save,'wb') as f:
                dill.dump(self.model_,f)
            f.close()

    def save_matrix(self,path_save = 'matrix'):

        if path_save is not None:
            with open(path_save,'wb') as f:
                dill.dump(self.mat_interaction,f)
                dill.dump(self.mat_item_feature,f)
                dill.dump(self.mat_user_feature,f)
            f.close()
    
    def save_mapping(self,path_save='mapping'):
        if path_save is not None:
            with open(path_save,'wb') as f:
                dill.dump(self.user_to_index,f)
                dill.dump(self.index_to_user,f)
                dill.dump(self.item_to_index,f)
                dill.dump(self.index_to_item,f)
                dill.dump(self.ufeat_to_index,f)
                dill.dump(self.index_to_ufeat,f)
                dill.dump(self.ifeat_to_index,f)
                dill.dump(self.index_to_ifeat,f)
            f.close()
            
    def save_new_matrix(self, path_save='new_matrix_item'):
        feature_item_new = self.spark.read.csv(self.parquet_dir + "new_item_features", header=True)
        new_item_list = np.array(np.unique(feature_item_new.select("item_id").collect()))
        itm_to_idx,idx_to_itm = self.mapping_index(new_item_list)

        row = np.array([itm_to_idx[x["item_id"]] for x in feature_item_new.collect()]).astype(int)
        col = np.array([self.ifeat_to_index[x["feature"]] for x in feature_item_new.collect()]).astype(int)
        X = len(set(row))
        Y = len(self.item_list_feature)
        matx = np.zeros((X,Y))

        rowb = set(row)
        rowb = list(rowb)
        for a, b in zip (row,col):
            matx[a][b] = 1
            
        mat_item_feature_new = sp.csc_matrix(matx)

        if path_save is not None:
            with open(path_save,'wb') as f:
                dill.dump(mat_item_feature_new,f)
                dill.dump(itm_to_idx,f)
                dill.dump(idx_to_itm,f)
            f.close()
