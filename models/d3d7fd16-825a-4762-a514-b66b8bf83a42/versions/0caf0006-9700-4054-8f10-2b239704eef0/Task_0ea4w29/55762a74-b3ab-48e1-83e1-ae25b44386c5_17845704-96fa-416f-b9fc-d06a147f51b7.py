import pickle
import pandas as pd
import json
import os
import inspect
import traceback
from xpms_helper.model.data_schema import DatasetFormat, DatasetConvertor
from xpms_helper.model import model_utils
from sklearn.metrics.scorer import SCORERS
from xpms_helper.model.train_info import TrainInfo
from xpms_helper.model.model_utils import calculate_metrics
from xpms_helper.model import dataset_utils, model_utils
import xgboost as xgb
from xpms_file_storage.file_handler import XpmsResourceFactory, XpmsResource, LocalResource
from sklearn.utils import class_weight
import numpy as np
from xpms_storage.utils import get_env
NAMESPACE = get_env("NAMESPACE", "claims-audit", False)
AMAZON_AWS_BUCKET = get_env("AMAZON_AWS_BUCKET", "xpms-ca-test", False)

def train(datasets,config):
    train_info = {"name" : "XGB"}
    result_dataset = run(datasets,config)
    return train_info, result_dataset

def get_params(config):
    try:
        params = config["algorithm"]["configuration"]
    except:
        params = dict()
    # this comes here

    if "additional_params" in config:
        for k in ["learning_rates","num_boost_round"]:
            params[k] = config["additional_params"][k]
        if "params" in  config["additional_params"]:
            params["params"].update(config["additional_params"]["params"])
    return params

def run_model(config, model_obj, X, en_classes, de_classes):
    class_indexes = {}
    for index in range(0, len(en_classes)):
        class_indexes[en_classes[index]] = index
    predictions = []
    # params = get_params(config)["params"]
    params = config["params"]
    predictions_raw = model_obj.predict(X)
    if params["objective"] in ["binary:logistic"]:
        for val in predictions_raw:
            row = [(1-val), val]
            predictions.append(row)
    elif params["objective"] in ["multi:softmax"]:
        for val in predictions_raw:
            row = [0] * len(en_classes)
            row[class_indexes[val]] = 1
            predictions.append(row)
    elif params["objective"] == "multi:softprob":
        predictions = predictions_raw
    else:
        raise Exception("unsupported objective parameter")

    result_df = pd.DataFrame(data=predictions, columns=de_classes)

    return result_df


def run(datasets, config, caching=None):
    dataset = DatasetConvertor.convert(datasets, DatasetFormat.DATA_FRAME, None)
    run_df = dataset["value"]
    target_column = "ec_dscr"
    X = run_df.loc[:, run_df.columns != target_column]

    dtest = xgb.DMatrix(data=X)

    file_name = "core_error_model_0.9.pkl"

    model_obj = model_utils.load(file_name=file_name, config=config, caching=caching)

    encoder = SimpleEncoder()

    encoder.fit(config, [], exec_mode="run")

    params = get_params(config)
    params["params"] = params.get("params", {})
    params["params"]["objective"] = "multi:softprob"

    result_df = run_model(params, model_obj, dtest, encoder.encoded_labels(), encoder.labels())

    result_dataset = {"value": result_df, "data_format": "data_frame"}
    return result_dataset


def evaluate(datasets, config, caching=None):
    dataset = DatasetConvertor.convert(datasets, DatasetFormat.DATA_FRAME, None)
    if "scorers" in config:
        scorers = config["scorers"]
    else:
        scorers = ["accuracy"]
    eval_df = dataset["value"]
    target_colum = "ec_dscr"

    y = eval_df[target_colum]

    model_output = run(datasets, config, caching=caching)
    # get prediction columns
    y_pred = model_output["value"].idxmax(axis=1).values
    score = calculate_metrics(dataset["value"], scorers, y, y_pred, config)
    return score, model_output


def retrain(datasets, config, caching=False):
    dataset = DatasetConvertor.convert(datasets, DatasetFormat.DATA_FRAME, None)
    train_df = dataset["value"]
    target_column = "ec_dscr"

    x = train_df.loc[:, train_df.columns != target_column]
    y = train_df[target_column]

    class_weights = list(class_weight.compute_class_weight('balanced', np.unique(y), y))
    w_array = np.ones(y.shape[0], dtype='float')
    for i, val in enumerate(y):
        w_array[i] = class_weights[val - 1]

    encoder = SimpleEncoder()
    y = encoder.fit(config, y, exec_mode="retrain")
    dtrain = xgb.DMatrix(x, label=y, weight=w_array)

    params = get_params(config)
    params["params"] = params.get("params", {})
    # if params["params"]["objective"] in ["multi:softmax","multi:softprob"]:
    #     params["params"]["num_class"] = len(set(y))

    params["params"]["objective"] = "multi:softprob"
    params["params"]["process_type"] = "update"
    params["params"]["updater"] = "refresh"
    params["params"]["refresh_leaf"] = True

    # load previous model
    file_name = "{0}.pkl".format("core_error_model_0.9")
    model_obj = model_utils.load(file_name=file_name, config=config, caching=caching)
    model_obj = xgb.train(dtrain=dtrain, xgb_model=model_obj, **params)
    file_name = "{}.pkl".format("core_error_model_0.9")
    model_utils.save(file_name=file_name, obj=model_obj, config=config)

    result_df = run_model(params, model_obj, dtrain, encoder.encoded_labels(), encoder.labels())
    full_dataset = dataset_utils.update_dataset({"0": datasets}, result_df)

    data = datasets['value']
    train_info = TrainInfo(
        **{"name": "", "path": config["src_dir"], "params": params, "classes": encoder.labels(), "rec": data.shape[0],
           "col": data.shape[1],
           "dep_var": target_column}).as_json()

    config["algorithm"] = dict(path=config["src_dir"])
    result_dataset = {"value": full_dataset, "data_format": "data_frame", "target_column": target_column,
                      "predicted_classes": encoder.labels()}
    return train_info, result_dataset

class SimpleEncoder:

    def labels(self):
        return self.labels_list

    def encoded_labels(self):
        return list(range(0, len(self.labels())))

    def defcode(self, config):
        pass

    def fit(self, config, Y, exec_mode = "train"):
        if exec_mode == "train":
            labels_list = list(set(Y))
            label_map = dict()
            for i in range(0, len(labels_list)):
                label_map[labels_list[i]] = i
            label_coding = {
                "labels_list": labels_list,
                "label_map": label_map
            }
            csv_minio_urn = "minio://{}/label_encoder/error_target_label_encoding".format(AMAZON_AWS_BUCKET)
            local_csv_path = "/tmp/error_target_label_encoding"
            minio_resource = XpmsResource.get(urn=csv_minio_urn)
            pickle.dump(label_coding,open(local_csv_path,"wb"))
            local_res = LocalResource(key=local_csv_path)
            local_res.copy(minio_resource)
            # model_utils.save("label_coding", label_coding, config)
        elif exec_mode in ["run", "eval"]:

            file_path = "minio://{}/label_encoder/error_target_label_encoding".format(AMAZON_AWS_BUCKET)
            local_pkl_path = "/tmp/error_target_label_encoding"
            minio_resource = XpmsResource.get(urn=file_path)
            local_res = LocalResource(key=local_pkl_path)
            minio_resource.copy(local_res)
            label_coding = pickle.load(open(local_pkl_path, "rb"))
            # label_coding = model_utils.load("label_coding", config)
            label_map = label_coding["label_map"]
            labels_list = label_coding["labels_list"]

        elif exec_mode == "retrain":
            file_path = "minio://{}/label_encoder/error_target_label_encoding".format(AMAZON_AWS_BUCKET)
            local_pkl_path = "/tmp/error_target_label_encoding"
            minio_resource = XpmsResource.get(urn=file_path)
            local_res = LocalResource(key=local_pkl_path)
            minio_resource.copy(local_res)
            label_coding = pickle.load(open(local_pkl_path, "rb"))

            # label_coding = model_utils.load("label_coding", config)
            labels_list = list(set(Y))
            for label in labels_list:
                if label not in label_coding["labels_list"]:
                    label_coding["label_map"][label] = len(label_coding["labels_list"])
                    label_coding["labels_list"].append(label)
            # model_utils.save("label_coding", label_coding, config)
            labels_list = label_coding["labels_list"]
            label_map = label_coding["label_map"]
        else:
            raise Exception("exec mode {} is not handled in the model runner script".format(exec_mode))

        y_ = []
        for label in Y:
            y_.append(label_map[label])

        self.label_map = label_map
        self.labels_list = labels_list

        return y_

def test_template():
    config={}
    config["storage"] = "local"
    config["src_dir"] = os.getcwd()
    dataset_obj = json.load(open(os.path.join(os.getcwd(),"datasets_obj/dataset_obj.json")))
    dataset_format = dataset_obj["data_format"]
    if dataset_format != "list":
        dataset_obj["value"] = LocalResource(key= os.path.join(os.getcwd(),"datasets")).urn
    train(dataset_obj,config)
    run(dataset_obj,config)
    evaluate(dataset_obj,config)

