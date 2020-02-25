import numpy as np
from lightfm import LightFM
from lightfm.data import Dataset
from lightfm.evaluation import auc_score
from lightfm.evaluation import precision_at_k
from scipy.sparse import coo_matrix
import scipy.sparse as sp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, lit, sum, coalesce, count, regexp_replace, split, explode
import os.path as path
import dill
spark = SparkSession.builder.master("local[*]").appName("hireko").getOrCreate()


# HIREKO (Hybrid Rekomendasi)
class Hireko(object):
    '''
    class ini berfungsi untuk memberikan rekomendasi dari interaksi existing
    dan juga memberikan rekomendasi kepada item-item baru yang tidak ada di interaksi 
    '''
    def __init__(self,**kwargs):
        
        self.model_path = kwargs.get('model_path','model')
        self.matrix_path = kwargs.get('matrix_path','matrix')
        self.mapping_path = kwargs.get('mapping_path','mapping')
        self.matrix_new_item_path = kwargs.get('matrix_new_item_path','')
        
        assert path.exists(self.model_path) == True
        assert path.exists(self.matrix_path) == True
        
        # new item jika ada
        if self.matrix_new_item_path:
            assert path.exists(self.matrix_new_item_path) == True
            with open (self.matrix_new_item_path,'rb') as f:
                self.matrix_new_item = dill.load(f)
                self.item_to_idx_new = dill.load(f)
                self.idx_to_item_new = dill.load(f)
        
        # model    
        with open(self.model_path,'rb') as f:
            self.model = dill.load(f)
        f.close()
        
        #matrix
        with open(self.matrix_path,'rb') as f:
            self.matrix_interactions = dill.load(f)
            self.matrix_item_feature = dill.load(f)
            self.matrix_user_feature = dill.load(f)
        f.close()
        
        #mapping
        with open(self.mapping_path,'rb') as f:
            self.user_to_index=dill.load(f)
            self.index_to_user=dill.load(f)
            self.item_to_index=dill.load(f)
            self.index_to_item=dill.load(f)
            self.ufeat_to_index=dill.load(f)
            self.index_to_ufeat=dill.load(f)
            self.ifeat_to_index=dill.load(f)
            self.index_to_ifeat=dill.load(f)
            
        f.close()
        
        self.list_user = np.array(list(self.user_to_index.keys()))
        self.list_item = np.array(list(self.item_to_index.keys()))
        self.list_user_feature = np.array(list(self.ufeat_to_index.keys()))
        self.list_item_feature = np.array(list(self.ifeat_to_index.keys()))

    
    
    def predict(self,user,feature):
        
        recommended_item = []
        recommended_new_item = []
        #warm user
        if user in self.list_user:
            recommended_item = self.hireko_user(user)
            if self.matrix_new_item_path:
                recommended_new_item = self.hireko_new_item(user)
            
        #cold user
        else:
            recommended_item = self.hireko_new_user(feature)

        return recommended_item,recommended_new_item


    def hireko_user(self,user):
        '''memberikan rekomendasi user existing ke item existing'''

        if not isinstance(user,str):
            str(user)
            

        #mencari item yang belum dilihat
        userindex = self.user_to_index[user]
        list_item_unwatched = np.delete(self.list_item,self.matrix_interactions.tocsr()[userindex].indices)

        itemindex = [self.item_to_index[x] for x in list_item_unwatched]

        scores = self.model.predict(
                userindex,
                itemindex,
                item_features=self.matrix_item_feature,
                user_features=self.matrix_user_feature)

        top_item = list_item_unwatched[[np.argsort(-scores)]][:5]
        top_score= scores[np.argsort(-scores)][:5]
        
        top_item_score = []
        for x in range(5):
            rekomendasi = {
                'item_id':top_item[x],
                'score':top_score[x]
            }
            top_item_score.append(rekomendasi)
            
        
        return top_item_score

    def hireko_new_user(self,feature_user):
        '''rekomendasi user baru (cold_user)'''

        if not isinstance(feature_user,str):
            str(feature_user)

        #di split
        feature = feature_user.split(',')

        matrix_new_user = self.create_user_feature_matrix(features=feature)
        
        itemindex = [self.item_to_index[x] for x in self.list_item]

        score = self.model.predict(
                0,
                itemindex,
                item_features=self.matrix_item_feature,
                user_features=matrix_new_user)

        top_item = self.list_item[[np.argsort(-score)]][:5]
        top_score= scores[np.argsort(-scores)][:5]
        
        top_item_score = []
        for x in range(5):
            rekomendasi = {
                'item_id':top_item[x],
                'score':top_score[x]
            }
            top_item_score.append(rekomendasi)

        return top_item_score

    
    def create_user_feature_matrix(self,features=[]):
        col = [self.ufeat_to_index[x] for x in features]
        Y = len(self.list_user_feature)
        matx = np.zeros((1,Y)) # bangun matrix zeros
        for a in col:
            matx[0][a]=1
        sparse = sp.csc_matrix(matx)
        return sparse
    
    def hireko_new_item(self,user):
        '''memberikan rekomendasi user existing ke new item'''

        #cek user existing
        assert (user in self.list_user) == True
        
        #list_new_item
        list_new_item = np.array(list(self.item_to_idx_new.keys()))
        userindex = self.user_to_index[user]
        itemindex = [self.item_to_idx_new[x] for x in list_new_item]

        scores = self.model.predict(
                userindex,
                itemindex,
                item_features=self.matrix_new_item,
                user_features=self.matrix_user_feature)

        top_item = list_new_item[[np.argsort(-scores)]][:5]
        top_score= scores[np.argsort(-scores)][:5]
        
        top_item_score = []
        for x in range(len(itemindex)):
            rekomendasi = {
                'item_id':top_item[x],
                'score':top_score[x]
            }
            top_item_score.append(rekomendasi)
            
        
        return top_item_score

        