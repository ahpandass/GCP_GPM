template_name='movie-etl-workflow'
cluster_name='shan-spark-cluster'
echo 1
gcloud dataproc workflow-templates create $template_name --region='asia-southeast1' &&
echo 2
gcloud dataproc workflow-templates set-managed-cluster $template_name --region='asia-southeast1' \
--cluster-name=$cluster_name \
--scopes=default \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 20 \
--num-workers 2 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 20 \
--image-version 1.3 &&
echo 3
gcloud dataproc workflow-templates add-job pyspark gs://shan_movie_bucket/movie_etl.py --region='asia-southeast1'\
--step-id=my-step_id \
--workflow-template=$template_name
echo 4
gcloud dataproc workflow-templates instantiate $template_name --region='asia-southeast1'
echo 5