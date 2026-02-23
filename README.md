# autoscaler

## API

### `/spanner/autoscaler`

SpannerのAutoscalerを起動します。

#### Request Body

```json
{
  "project": "your-gcp-project-id",
  "instance": "your-spanner-instance-id",
  "puStep": 100,
  "puMin": 100,
  "puMax": 1000,
  "scaleUpThreshold": 65.0,
  "scaleDownThreshold": 20.0
}
```
