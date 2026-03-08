#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import * as fs from 'fs';
import * as path from 'path';
import { BlueskyFeedJpStack } from '../lib/bluesky-feed-jp-stack';

// Load .env file
const envPath = path.join(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf-8');
  envContent.split('\n').forEach(line => {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#')) {
      const [key, value] = trimmed.split('=');
      if (key && value) {
        process.env[key] = value;
      }
    }
  });
}

const app = new cdk.App();
const account = process.env.CDK_DEFAULT_ACCOUNT || '878311109818';
const region = process.env.CDK_DEFAULT_REGION || 'ap-northeast-1';

new BlueskyFeedJpStack(app, 'BlueskyFeedJpStack', {
  env: { account, region },
});
