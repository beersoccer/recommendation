# 基于 Apache Spark 和 Elasticsearch 的可伸缩的推荐系统

本系统通过存储在 Elasticsearch 中的用户行为数据，使用 Spark 来训练一个协同过滤推荐模型，并将训练好的模型保存到 Elasticsearch，然后使用 Elasticsearch 通过该模型来提供实时推荐。以此达到离线训练，在线推荐的目的。

因为使用了 Elasticsearch 插件，系统除了能够提供个性化用户和类似条目推荐，还能够将推荐与搜索和内容过滤相结合。

本文主要描述了如下内容：

* 使用 Elasticsearch Spark 连接器将用户行为数据导入到 Elasticsearch 中并建立索引。
* 将行为数据加载到 Spark DataFrames 中，并使用 Spark 的机器学习库 (MLlib) 来训练一个协同过滤推荐系统模型。
* 将训练后的模型导出到 Elasticsearch 中。
* 使用一个自定义 Elasticsearch 插件，计算 _个性化用户_ 和 _类似条目_ 推荐，并将推荐与搜索和内容过滤相结合。

![架构图](doc/image/architecture.png)

## 操作流程
1. 将数据集加载到 Spark 中。
2. 使用 Spark DataFrame 操作清理该数据集，并将它加载到 Elasticsearch 中。
3. 使用 Spark MLlib，离线训练一个协同过滤推荐模型。
4. 将得到的模型保存到 Elasticsearch 中。
5. 使用 Elasticsearch 查询和一个自定义矢量评分插件，在线生成用户推荐。

## 包含的组件
* [Apache Spark](http://spark.apache.org/)：一个开源、快速、通用的集群计算系统。
* [Elasticsearch](http://elasticsearch.org)：开源搜索和分析引擎。
* [Elasticsearch Vector Scoring](https://github.com/MLnick/elasticsearch-vector-scoring)：A plugin allows you to score documents based on arbitrary raw vectors, using dot product or cosine similarity.

# 步骤
按照以下步骤部署所需的部件，并创建推荐服务。

1. [设置 Elasticsearch](#2-set-up-elasticsearch)
2. [下载 Elasticsearch Spark 连接器](#3-download-the-elasticsearch-spark-connector)
3. [下载 Apache Spark](#4-download-apache-spark)
4. [数据准备](#5-prepare-the-data)
5. [启动 Notebook](#6-launch-the-notebook)
6. [运行 Notebook](#7-run-the-notebook)

### 1.设置 Elasticsearch

此推荐系统目前依赖于 Elasticsearch 5.3.0。转到[下载页面](https://www.elastic.co/downloads/past-releases/elasticsearch-5-3-0)，下载适合您的系统的包。

如果在 Linux / Mac 上，可以下载 [TAR 归档文件](https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.3.0.tar.gz) 并使用以下命令进行解压：

```
$ wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.3.0.tar.gz
$ tar xfz elasticsearch-5.3.0.tar.gz
```

使用以下命令，将目录更改为新解压的文件夹：

```
$ cd elasticsearch-5.3.0
```

接下来安装 [Elasticsearch 矢量评分插件](https://github.com/MLnick/elasticsearch-vector-scoring)。运行以下命令（Elasticsearch 将为您下载插件文件）：

```
$ ./bin/elasticsearch-plugin install https://github.com/MLnick/elasticsearch-vector-scoring/releases/download/v5.3.0/elasticsearch-vector-scoring-5.3.0.zip
```

接下来，启动 Elasticsearch（在一个单独的终端窗口中这么做，使其保持正常运行）：

```
$ ./bin/elasticsearch
```

您会看到一些显示的启动日志。如果显示 `elasticsearch-vector-scoring-plugin` 则插件已成功加载：

```
$ ./bin/elasticsearch
[2017-09-08T15:58:18,781][INFO ][o.e.n.Node               ] [] initializing ...
...
[2017-09-08T15:58:19,406][INFO ][o.e.p.PluginsService     ] [2Zs8kW3] loaded plugin [elasticsearch-vector-scoring]
[2017-09-08T15:58:20,676][INFO ][o.e.n.Node               ] initialized
...
```

最后安装 Elasticsearch Python 客户端。运行以下命令（执行此命令时使用的终端窗口应该与运行 Elasticsearch 的终端窗口不同）：

```
$ pip install elasticsearch
```

如果系统安装了Anaconda，也可以使用conda install命令来安装。

### 2.下载 Elasticsearch Spark 连接器

[Elasticsearch Hadoop 项目](https://www.elastic.co/products/hadoop) 提供了 Elasticsearch 与各种兼容 Hadoop 的系统（包括 Spark）之间的连接器。该项目提供了一个 ZIP 文件供下载，其中包含所有这些连接器。此推荐系统需要将特定于 Spark 的连接器 JAR 文件放在类路径上。按照以下步骤设置连接器：

1.[下载](http://download.elastic.co/hadoop/elasticsearch-hadoop-5.3.0.zip) `elasticsearch-hadoop-5.3.0.zip` 文件，其中包含所有连接器。为此，可以运行：
```
$ wget http://download.elastic.co/hadoop/elasticsearch-hadoop-5.3.0.zip
```
2.运行以下命令来解压该文件：
```
$ unzip elasticsearch-hadoop-5.3.0.zip
```
3.Spark 连接器的 JAR 名为 `elasticsearch-spark-20_2.11-5.3.0.jar`，它将位于您解压上述文件所用的目录的 `dist` 子文件夹中。

### 3.下载 Apache Spark

本推荐系统适用于任何 Spark 2.x 版本，从[下载页面](http://spark.apache.org/downloads.html) 下载最新版 Spark（目前为 2.2.0）。运行以下命令来解压它：
```
$ tar xfz spark-2.2.0-bin-hadoop2.7.tgz
```

> *请注意，如果下载不同的版本，应该相应地调整上面使用的相关命令和其他地方。*

![下载 Apache Spark](doc/image/download-apache-spark.png)

Spark 的机器学习库 [MLlib](http://spark.apache.org/mllib)依赖于 [Numpy](http://www.numpy.org)，运行以下命令安装 Numpy：
```
$ pip install numpy
```

### 4.数据准备

推荐系统依赖于一个用户行为数据集，用户行为包括浏览、购买资源。如果只记录了购买行为，那么暂时就只以此行为构建数据集。

数据集中的每条数据是一个多元组，其中包括：
* userId，全局唯一的用户Id，这里的用户是指真实购买资源的广告主，而不是在系统中进行操作的代理商。
* itemId，全局唯一的资源Id，这里的资源是指可以展示广告的广告位。
* rating，用户评分，系统目前没有真实的用户评分行为，我们将用户的浏览、购买行为等价为评分。一次浏览为3分，一次购买为5分。
* timeStamp，时间戳，用户产生这次行为的时间，时间精确到秒。

数据文件的格式可以参考 [Movielens 数据集](https://grouplens.org/datasets/movielens/)，其中包含一组电影用户提供的评分和电影元数据。我们的数据集可以完全照此准备。

### 5.启动 Notebook

> 该 Notebook 应该适用于 Python 2.7 或 3.x（而且已在 2.7.11 和 3.6.1 上测试）

要运行该 Notebook，您需要在一个 Jupyter Notebook 中启动一个 PySpark 会话。如果没有安装 Jupyter，可以运行以下命令来安装它：
```
$ pip install jupyter
```

在启动 Notebook 时，记得在类路径上包含来自[第 3 步](#3-download-the-elasticsearch-spark-connector) 的 Elasticsearch Spark 连接器 JAR。

运行以下命令，以便在本地启动您的 PySpark Notebook 服务器。**要正确运行此命令，需要从您在[第 1 步](#1-clone-the-repo)克隆的 Code Pattern 存储库的基本目录启动该 Notebook**。如果您不在该目录中，请先通过 `cd` 命令进入该目录。

```
PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" ../spark-2.2.0-bin-hadoop2.7/bin/pyspark --driver-memory 4g --driver-class-path ../../elasticsearch-hadoop-5.3.0/dist/elasticsearch-spark-20_2.11-5.3.0.jar
```

这会打开一个浏览器窗口，其中显示了 Code Pattern 文件夹内容。单击 `notebooks` 子文件夹，然后单击 `elasticsearch-spark-recommender.ipynb` 文件启动该 Notebook。

![启动 Notebook](doc/image/launch-notebook.png)

> _可选：_
>
> 要在推荐演示中显示图像，您需要访问 [The Movie Database API](https://www.themoviedb.org/documentation/api)。请按照[操作说明](https://developers.themoviedb.org/3/getting-started) 获取 API 密钥。您还需要使用以下命令安装 Python 客户端：
```
$ pip install tmdbsimple
```
>
> 不访问此 API 也能执行该演示，但不会显示图像（所以看起来不太美观！）。

### 6.运行该 Notebook

执行一个 Notebook 时，实际情况是，
按从上往下的顺序执行该 Notebook 中的每个代码单元。

可以选择每个代码单元，并在代码单元前面的左侧空白处添加一个标记。标记
格式为 `In [x]:`。根据 Notebook 的状态，`x` 可以是：

* 空白，表示该单元从未执行过。
* 一个数字，表示执行此代码步骤的相对顺序。
* 一个`*`，表示目前正在执行该单元。

可通过多种方式执行 Notebook 中的代码单元：

* 一次一个单元。
  * 选择该单元，然后在工具栏中按下 `Play` 按钮。也可以按下 `Shift+Enter` 来执行该单元并前进到下一个单元。
* 批处理模式，按顺序执行。
  * `Cell` 菜单栏中包含多个选项。例如，可以
    选择 `Run All` 运行 Notebook 中的所有单元，也可以选择 `Run All Below`，
    这将从当前选定单元下方的第一个单元开始执行，然后
    继续执行后面的所有单元。

![](doc/source/images/notebook-run-cells.png)

# 样本输出

`data/examples` 文件夹中的示例输出显示了运行 Notebook 后的完整输出。可以在[这里]()查看它。

> *备注：* 要查看代码和没有输出的精简单元，可以在 [Github 查看器中查看原始 Notebook](notebooks/elasticsearch-spark-recommender.ipynb)。

# 故障排除

* 错误：`java.lang.ClassNotFoundException: Failed to find data source: es.`

如果尝试在 Notebook 中将数据从 Spark 写入 Elasticsearch 时看到此错误，这意味着 Spark 在启动该 Notebook 时没有在类路径中找到 Elasticsearch Spark 连接器 (`elasticsearch-spark-20_2.11-5.3.0.jar`)。

  > 解决方案：首先启动[第 6 步](#6-launch-the-notebook) 中的命令，**确保从 Code Pattern 存储库的基本目录运行它**。

  > 如果这不起作用，可以尝试在启动 Notebook 时使用完全限定的 JAR 文件路径，比如：
  > `PYSPARK_DRIVER_PYTHON="jupyter" PYSPARK_DRIVER_PYTHON_OPTS="notebook" ../spark-2.2.0-bin-hadoop2.7/bin/pyspark --driver-memory 4g --driver-class-path /FULL_PATH/elasticsearch-hadoop-5.3.0/dist/elasticsearch-spark-20_2.11-5.3.0.jar`
  > 其中 `FULL_PATH` 是 _您解压 `elasticsearch-hadoop` ZIP 文件_ 所用的目录的完全限定（不是相对）路径。

* 错误：`org.elasticsearch.hadoop.EsHadoopIllegalStateException: SaveMode is set to ErrorIfExists and index demo/ratings exists and contains data.Consider changing the SaveMode`

如果尝试在 Notebook 中将数据从 Spark 写入 Elasticsearch 时看到此错误，这意味着您已将数据写入到相关索引（例如将评分数据写入到 `ratings` 索引中）。

  > 解决方案：尝试从下一个单元继续处理该 Notebook。也可以先删除所有索引，重新运行 Elasticsearch 命令来创建索引映射（参阅 Notebook 中的小节 *第 2 步：将数据加载到 Elasticsearch 中*）。

* 错误：`ConnectionRefusedError: [Errno 61] Connection refused`

尝试在 Notebook 中连接到 Elasticsearch 时可能看到此错误。这可能意味着您的 Elasticsearch 实例未运行。

 > 解决方案：在一个新终端窗口中，通过 `cd` 命令转到安装 Elasticsearch 的目录，运行 `./bin/elasticsearch` 来启动 Elasticsearch。

* 错误：`Py4JJavaError: An error occurred while calling o130.save.
: org.elasticsearch.hadoop.rest.EsHadoopNoNodesLeftException: Connection error (check network and/or proxy settings)- all nodes failed; tried [[127.0.0.1:9200]]`

尝试在 Notebook 中将数据从 Elasticsearch 读入 Spark 中（或将数据从 Spark 写入 Elasticsearch 中）时，可能看到此错误。这可能意味着您的 Elasticsearch 实例未运行。

 > 解决方案：在一个新终端窗口中，通过 `cd` 命令转到安装 Elasticsearch 的目录，运行 `./bin/elasticsearch` 来启动 Elasticsearch。

* 错误：`ImportError: No module named elasticsearch`

如果您遇到此错误，这意味着 Elasticsearch Python 客户端未安装，或者可能无法在 `PYTHONPATH` 上找到。

 > 解决方案：先尝试使用 `$ pip install elasticsearch` 安装该客户端（如果在 Python 虚拟环境中运行，比如 Conda 或 Virtualenv），或者使用 `$ sudo pip install elasticsearch`。
 > 如果这样不起作用，可以将您的 site-packages 文件夹添加到 Python 路径（比如在 Mac 上：`export PYTHONPATH=/Library/Python/2.7/site-packages` 用于 Python 2.7）。有关 Linux 上的另一个示例，参阅这个 [Stackoverflow 问题](https://stackoverflow.com/questions/7731947/add-module-to-pythonpath-nothing-works)。
 > _备注：_ 同一个通用解决方案适用于您可能遇到的任何其他模块导入错误。

 * 错误：`HTTPError: 401 Client Error: Unauthorized for url: https://api.themoviedb.org/3/movie/1893?api_key=...`

如果在测试 TMDb API 访问或生成推荐时，在您的 Notebook 中看到此错误，这意味着您已经安装了 `tmdbsimple` Python 包，但没有设置您的 API 密钥。

> 解决方案：按照[第 6 步](#6-launch-the-notebook) 末尾的操作说明来设置您的 TMDb 帐户并获取您的 API 密钥。然后将该密钥复制到 _第 1 步：准备数据_ 结束后 Notebook 单元中的 `tmdb.API_KEY = 'YOUR_API_KEY'` 行中（即将 `YOR_API_KEY` 替换为正确的密钥）。完成这一步后，执行该单元来测试您对 TMDb API 的访问。

# 链接

* [优酷上的演示](http://v.youku.com/v_show/id_XMzYwNjg4MjkwNA==.html)：观看视频。
* [站立会议视频演示](https://youtu.be/sa_Y488vj0M)：观看涵盖本 code pattern 的一些背景和技术细节的站立会议演示。
* [站立会议资料](https://www.slideshare.net/sparktc/spark-ml-meedup-pentreath-puget)：查看演示的幻灯片。
* [ApacheCon Big Data Europe 2016](http://events.linuxfoundation.org/sites/events/files/slides/ApacheBigDataEU16-NPentreath.pdf)：查阅该站立会议演示的一个扩展版本：
* [数据和分析](https://www.ibm.com/cloud/garage/content/architecture/dataAnalyticsArchitecture)：了解如何将本模式融入到数据和分析参考架构中。

# 了解更多信息

* **数据分析 Code Pattern**：喜欢本 Code Pattern 吗？了解我们的其他[数据分析 Code Pattern](https://developer.ibm.com/cn/technologies/data-science/)
* **AI 和数据 Code Pattern 播放清单**：收藏包含我们所有 Code Pattern 视频的[播放清单](http://i.youku.com/i/UNTI2NTA2NTAw/videos?spm=a2hzp.8244740.0.0)
* **Data Science Experience**：通过 IBM [Data Science Experience](https://datascience.ibm.com/) 掌握数据科学艺术
* **IBM Cloud 上的 Spark**：需要一个 Spark 集群？通过我们的 [Spark 服务](https://console.bluemix.net/catalog/services/apache-spark)，在 IBM Cloud 上创建多达 30 个 Spark 执行程序。

# 许可
[Apache 2.0](LICENSE)


from sklearn.metrics.pairwise import cosine_similarity
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import string
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfTransformer
import nltk
import pickle
设置数据文件的路径，载入nltk的stopwords库和wordnet库。

WordNet is a lexical database of English. Using synsets, helps find conceptual relationships between words such as hypernyms, hyponyms, synonyms, antonyms etc. Here is a API description of it.

data_path = "../data/"

try:
    nltk.data.find('corpora/stopwords.zip/stopwords/')
except LookupError as e:
    print(e)
    # nltk.set_proxy('http://proxyus2.huawei.com:8080')
    nltk.download('corpora/stopwords.zip')

try:
    nltk.data.find('corpora/wordnet.zip/wordnet/')
except LookupError as e:
    print(e)
    # nltk.set_proxy('http://proxyus2.huawei.com:8080')
    nltk.download('corpora/wordnet.zip')
定义一组文本处理的函数：

remove_stopwords：去除停用词。
wordnet：使用WordNetLemmatizer提取词干。WordNet Lemmatizer using WordNet's built-in morphy function to retrieve the word's stem. Returns the input word unchanged if it cannot be found in WordNet.
loadfile：文件按行读入一个列表。
lines_processing：继续按行处理上一步读入的文件，去除停用词、提取词干，最后写入训练集或测试集文件。
def remove_stopwords(word_list):
    stop_words = stopwords.words('english')
    filter_words = [w for w in word_list if not w in stop_words]
    stopWords =  [w for w in word_list if w in stop_words]
    stopWords = " ".join(stopWords) + '\n'
    with open(data_path + "stopwords.txt", "a") as f:
        f.write(stopWords)
    return filter_words

# 使用WordNetLemmatizer提取词干
def wordnet(sentence, stopwords = True):
    wordnet_lemmatizer = WordNetLemmatizer()
    words = [wordnet_lemmatizer.lemmatize(word, 'n').lower() for word in sentence.split()]
    words = [wordnet_lemmatizer.lemmatize(word, 'a').lower() for word in words]
    words = [wordnet_lemmatizer.lemmatize(word, 'v').lower() for word in words]
    translation_table = str.maketrans(string.punctuation + string.ascii_uppercase + string.digits,
                                      " " * len(string.punctuation) + string.ascii_lowercase + " " * len(string.digits))
    if stopwords:
        words = remove_stopwords(words)
    words = [word for word in words]
    new_sent =  ' '.join(words)
    new_sent = new_sent.translate(translation_table)
    return new_sent

def loadfile(filename):
    new_lines = []
    with open(filename, "r") as f:
        content = f.read()
        lines = content.split("\n")
        for line in lines:
            new_lines.append(line.split("\t"))
    return new_lines

def lines_processing(lines, stopwords = True):
    new_content = ""
    line_len = len(lines[0]) > 3
    for line in lines:
        sen1 = wordnet(line[1], stopwords)
        sen2 = wordnet(line[2], stopwords)
        if line_len:
            line = line[0] + "\t" + sen1 + "\t" + sen2 + "\t" + line[3] +"\n"
        else:
            line = line[0] + "\t" + sen1 + "\t" + sen2 + "\n"
        new_content += line
    if line_len:
        with open(data_path + "train.txt","w") as f:
            f.write(new_content)
    else:
        with open(data_path + "test.txt","w") as f:
            f.write(new_content)
准备训练集数据文件。

lines = loadfile(data_path + "train_ai-lab.txt")
print(lines[:2])

lines_processing(lines = lines,stopwords = True)
[['10001', 'two big brown dogs running through the snow.', 'A brown dog running through the grass.', '2.00000'], ['10002', 'A woman is peeling a potato.', 'A woman is slicing a tomato.', '1.33300']]
准备测试集数据文件。

lines = loadfile(data_path + "test_ai-lab.txt")
print(lines[:2])

lines_processing(lines = lines,stopwords = True)
[['11501', 'A man in a blue shirt is running in a stadium field.', 'A man wearing a blue hat and shirt is riding a white horse.'], ['11502', 'Two cartoon rabbits are kissing.', 'A rabbit is kissing another rabbit.']]
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import TfidfTransformer
from nltk.corpus.reader import WordNetError
from sklearn.metrics import r2_score, make_scorer
from sklearn.model_selection import GridSearchCV, train_test_split, cross_val_score, KFold
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR
from nltk.stem import WordNetLemmatizer
from nltk import FreqDist
from nltk.corpus import brown, wordnet, stopwords
from scipy.stats import pearsonr, spearmanr
from matplotlib import pyplot as plt
import nltk
import numpy as np
import pickle
import nltk
import numpy as np
import re
import os
import seaborn as sns
import pandas as pd
# nert_tagger = StanfordNERTagger('english.all.3class.distsim.crf.ser.gz')
# pos_tagger = StanfordPOSTagger('english-bidirectional-distsim.tagger')
# parser = StanfordParser('edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz')

def vertorlize(content):
    vectorizer = CountVectorizer()
    X = vectorizer.fit(content)
    return X.toarray()


def bag_of_words(sen1,sen2):
    return  cosine_similarity([sen1],[sen2])[0][0]

def topic_id(all_sens):
    lda = LatentDirichletAllocation(n_topics=6,
                                    learning_offset=50.,
                                    random_state=0)
    docres = lda.fit_transform(all_sens)
    return docres

def tf_idf(sen1,sen2):
#     print("s1:",sen1,"s2:",sen2)
    transformer = TfidfTransformer()
    tf_idf = transformer.fit_transform([sen1,sen2]).toarray()
    return tf_idf[0], tf_idf[1]


def lcs_dp(input_x, input_y):
    # input_y as column, input_x as row
    dp = [([0] * len(input_y)) for i in range(len(input_x))]
    maxlen = maxindex = 0
    for i in range(0, len(input_x)):
        for j in range(0, len(input_y)):
            if input_x[i] == input_y[j]:
                if i != 0 and j != 0:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                if i == 0 or j == 0:
                    dp[i][j] = 1
                if dp[i][j] > maxlen:
                    maxlen = dp[i][j]
                    maxindex = i + 1 - maxlen
                    # print('最长公共子串的长度是:%s' % maxlen)
                    # print('最长公共子串是:%s' % input_x[maxindex:maxindex + maxlen])
    return maxlen,input_x[maxindex:maxindex + maxlen]

def not_empty(s):
    return s and s.strip()


def tag_and_parser(str_sen1,str_sen2):
#     print(str_sen1.split(" "),str_sen2)
    sen1 = list(filter(not_empty, str_sen1.split(" ")))
    sen2 = list(filter(not_empty, str_sen2.split(" ")))
    # print("sen1:",sen1)
    post_sen1 = nltk.pos_tag(sen1)
    post_sen2 = nltk.pos_tag(sen2)
    pos1 ,pos2 = "", ""
    # print(post_sen1,post_sen2)
    for word,pos in post_sen1:
        pos1 += pos+" "
    for word,pos in post_sen2:
        pos2 += pos+" "
    # print(pos1,pos2)
    maxlen, subseq = lcs_dp(pos1,pos2)
    return  len(subseq.split(" "))/len(str_sen1.split(' ')), len(subseq.split(" "))/len(str_sen2.split(' '))


def all_features(content):
    vectorlize = CountVectorizer()
    sents = []
    sents1 = []
    sents2 = []
    scores = []
    for line in content[:-1]:
    #         print(line[1])
        sents1.append(line[1])
        sents2.append(line[2])
        sents.append(line[1])
        sents.append(line[2])
        if len(line) > 3:
            scores.append(float(line[3]))

    Sents = vectorlize.fit_transform(sents).toarray()
    Sents1 = vectorlize.transform(sents1).toarray()
    Sents2 = vectorlize.transform(sents2).toarray()
    with open("model.pickle","wb") as f:
        pickle.dump(vectorlize, f)
    tfidf_Sents1 = []
    tfidf_Sents2 = []
    tfidf_Sents = []
    cosine = []
    pos_lcs = []
    for i in range(len(sents1)):
        tfidf_Sent1, tfidf_Sent2 = tf_idf(Sents1[i],Sents2[i])
        tfidf_Sents1.append(tfidf_Sent1)
        tfidf_Sents2.append(tfidf_Sent2)
        tfidf_Sents.append(tfidf_Sent1)
        tfidf_Sents.append(tfidf_Sent2)
        cosine.append(cosine_similarity([tfidf_Sent1],[tfidf_Sent2])[0][0])
        lcs1, lcs2 = tag_and_parser(sents1[i],sents2[i])
        pos_lcs.append([lcs1, lcs2])
    tp_Sents = topic_id(tfidf_Sents)
    return cosine, pos_lcs,tfidf_Sents1,tfidf_Sents2,tp_Sents,scores


def get_words(sentence):
    return [i.strip('., ') for i in sentence.split(' ')]


def get_ngram(word, n):
    ngrams = []
    word_len = len(word)
    for i in range(word_len - n + 1):
        ngrams.append(word[i: i + n])
    return ngrams


def get_lists_intersection(s1, s2):
    s1_s2 = []
    for i in s1:
        if i in s2:
            s1_s2.append(i)
    return s1_s2


def overlap(sentence1_ngrams, sentence2_ngrams):
    s1_len = len(sentence1_ngrams)
    s2_len = len(sentence2_ngrams)
    if s1_len == 0 and s2_len == 0:
        return 0
    s1_s2_len = max(1, len(get_lists_intersection(sentence2_ngrams, sentence1_ngrams)))
    return 2 / (s1_len / s1_s2_len + s2_len / s1_s2_len)


def get_ngram_feature(sentence1, sentence2, n):
    sentence1_ngrams = []
    sentence2_ngrams = []

    for word in sentence1:
        sentence1_ngrams.extend(get_ngram(word, n))

    for word in sentence2:
        sentence2_ngrams.extend(get_ngram(word, n))

    return overlap(sentence1_ngrams, sentence2_ngrams)


def is_subset(s1, s2):
    for i in s1:
        if i not in s2:
            return False
    return True


def get_numbers_feature(sentence1, sentence2):
    s1_numbers = [float(i) for i in re.findall(r"[-+]?\d+\.?\d*", " ".join(sentence1))]
    s2_numbers = [float(i) for i in re.findall(r"[-+]?\d+\.?\d*", " ".join(sentence2))]
    s1_s2_numbers = []
    for i in s1_numbers:
        if i in s2_numbers:
            s1_s2_numbers.append(i)

    s1ands2 = max(len(s1_numbers) + len(s2_numbers), 1)
    return [np.log(1 + s1ands2), 2 * len(s1_s2_numbers) / s1ands2,
            is_subset(s1_numbers, s2_numbers) or is_subset(s2_numbers, s1_numbers)]


def get_shallow_features(sentence):
    counter = 0
    for word in sentence:
        if len(word) > 1 and (re.match("[A-Z].*]", word) or re.match("\.[A-Z]+]", word)):
            counter += 1
    return counter


def get_word_embedding(inf_content, word):
    if inf_content:
        return np.multiply(information_content(word), embeddings.get(word, np.zeros(300)))
    else:
        return embeddings.get(word, np.zeros(300))


def sum_embeddings(words, inf_content):
    vec = get_word_embedding(inf_content, words[0])
    for word in words[1:]:
        vec = np.add(vec, get_word_embedding(inf_content, word))
    return vec


def word_embeddings_feature(sentence1, sentence2):
    return cosine_similarity(unpack(sum_embeddings(sentence1, False)),
                             unpack(sum_embeddings(sentence2, False)))[0][0]


def information_content(word):
    return np.log(all_words_count / max(1, frequency_list[word]))


def unpack(param):
    return param.reshape(1, -1)


def weighted_word_embeddings_feature(sentence1, sentence2):
    return cosine_similarity(unpack(sum_embeddings(sentence1, True)),
                             unpack(sum_embeddings(sentence2, True)))[0][0]


def weighted_word_coverage(s1, s2):
    s1_s2 = get_lists_intersection(s1, s2)
    return np.sum([information_content(i) for i in s1_s2]) / np.sum([information_content(i) for i in s2])


def harmonic_mean(s1, s2):
    if s1 == 0 or s2 == 0:
        return 0
    return s1 * s2 / (s1 + s2)


def get_wordnet_pos(treebank_tag):
    if treebank_tag.startswith('A') or treebank_tag.startswith('JJ'):
        return wordnet.ADJ
    elif treebank_tag.startswith('V'):
        return wordnet.VERB
    elif treebank_tag.startswith('N'):
        return wordnet.NOUN
    elif treebank_tag.startswith('R'):
        return wordnet.ADV
    else:
        return ''


def get_synset(word):
    try:
        return wordnet.synset(word + "." + get_wordnet_pos(tagger([word])[0][1]) + ".01")
    except :
        return 0


def wordnet_score(word, s2):
    if word in s2:
        return 1
    else:
        similarities = []
        for w in s2:
            try:
                value = get_synset(word).path_similarity(get_synset(w))
                if value is None:
                    value = 0
                similarities.append(value)
            except AttributeError:
                similarities.append(0)
        return np.max(similarities)


def wordnet_overlap(s1, s2):
    suma = 0
    for w in s1:
        suma += wordnet_score(w, s2)
    return suma / len(s2)


def feature_vector(a, b):
    fvec = []
    # Ngram overlap

    fvec.append(get_ngram_feature(a, b, 1))
    fvec.append(get_ngram_feature(a, b, 2))
    fvec.append(get_ngram_feature(a, b, 3))

    # WordNet-aug. overlap -
    fvec.append(harmonic_mean(wordnet_overlap(a, b), wordnet_overlap(b, a)))

    # Weighted word overlap -
    fvec.append(harmonic_mean(weighted_word_coverage(a, b),
                              weighted_word_coverage(b, a)))
    # sentence num_of_words differences -
    fvec.append(abs(len(a) - len(b)))

    # summed word embeddings - lagano
    fvec.append(word_embeddings_feature(a, b))
    fvec.append(weighted_word_embeddings_feature(a, b))

    # Shallow NERC - lagano
    fvec.append(get_shallow_features(a))
    fvec.append(get_shallow_features(b))

    # Numbers overlap - returns list of 3 features
    fvec.extend(get_numbers_feature(a, b))
    return fvec

def extract():
    content1 = loadfile('train.txt')
    cosine, pos_lcs, tfidf_Sents1, tfidf_Sents2, tp_Sents, scores = all_features(content1)
    tp_Sents = np.array(tp_Sents)
    cosine = np.array(cosine)
    pos_lcs = np.array(pos_lcs)
    tp_Sents = np.array(tp_Sents)
    X_train = np.c_[cosine,pos_lcs,tp_Sents[::2],tp_Sents[1::2]]
    Y_train = np.array(scores)

    content2 = loadfile("test.txt")
    Y_ids = []
    for line in content2:
        Y_ids.append(line[0])
    cosine, pos_lcs, tfidf_Sents1, tfidf_Sents2, tp_Sents, scores = all_features(content2)
    tp_Sents = np.array(tp_Sents)
    cosine = np.array(cosine)
    pos_lcs = np.array(pos_lcs)
    tp_Sents = np.array(tp_Sents)
    X_test = np.c_[cosine, pos_lcs, tp_Sents[::2], tp_Sents[1::2]]
    return X_train, Y_train, X_test, Y_ids
提取预训练的词向量并生成词典。

在NLP中经常需要将词汇向量化，常见的方向有Word2vec和GloVe。后者是斯坦福大学的开源实现，提供了预训练的词汇向量文件供下载使用，“glove.txt”就是其的一个很小的子集。

with open(data_path + 'glove.txt', 'r') as f:
    embeddings = {}
    for line in f.readlines():
        args = get_words(line.strip("\n\t "))
        embeddings[args[0]] = [float(i) for i in args[1:]]
wordnet_lemmatizer = WordNetLemmatizer()
stopwords = stopwords.words("english")

tagger = nltk.tag.pos_tag
frequency_list = FreqDist(i.lower() for i in brown.words())
all_words_count = 0
for i in frequency_list:
    all_words_count += frequency_list[i]
