import { Injectable, OnModuleInit, Logger, Inject } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { ConfigService } from '@nestjs/config';
import { parse } from 'csv-parse';
import { createReadStream } from 'fs';
import { v4 as uuidv4 } from 'uuid';
import {
  CheckinEvent,
  CheckinEventSchema,
  CHECKIN_TOPIC,
  WorkoutType,
} from './schemas/checkin-event.schema';

@Injectable()
export class KafkaProducerService implements OnModuleInit {
  private readonly logger = new Logger(KafkaProducerService.name);

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafka: ClientKafka,
    private readonly config: ConfigService,
  ) {}

  async onModuleInit() {
    await this.kafka.connect();
    this.logger.log('Kafka producer connected');
  }

  // ── Publish a single validated event ──────────────────────────────────────
  async publishCheckinEvent(event: CheckinEvent): Promise<void> {
    const validated = CheckinEventSchema.parse(event); // throws if invalid
    await this.kafka
      .emit(CHECKIN_TOPIC, {
        key: validated.userId, // partition key — ensures ordered delivery per user
        value: JSON.stringify(validated),
      })
      .toPromise();
  }

  // ── Replay CSV file as a Kafka event stream ────────────────────────────────
  // Simulates real-time ingestion from the Kaggle checkin CSV
  // delayMs: artificial delay between events (default 10ms — ~100 events/sec)
  async replayCsvAsStream(
    csvPath: string,
    delayMs = 10,
    limit?: number,
  ): Promise<void> {
    this.logger.log(`Starting CSV replay from ${csvPath}`);
    let count = 0;

    const parser = createReadStream(csvPath).pipe(
      parse({ columns: true, skip_empty_lines: true }),
    );

    for await (const row of parser) {
      if (limit && count >= limit) break;

      const durationMins =
        (new Date(row.checkout_time).getTime() -
          new Date(row.checkin_time).getTime()) /
        60000;

      const event: CheckinEvent = {
        eventId: uuidv4(),
        userId: row.user_id,
        gymId: row.gym_id,
        checkinTime: new Date(row.checkin_time).toISOString(),
        checkoutTime: new Date(row.checkout_time).toISOString(),
        workoutType: row.workout_type as WorkoutType,
        caloriesBurned: parseFloat(row.calories_burned),
        durationMins,
        source: 'csv-replay',
      };

      await this.publishCheckinEvent(event);
      count++;

      if (delayMs > 0) await this.sleep(delayMs);
      if (count % 1000 === 0) {
        this.logger.log(`Replayed ${count} events...`);
      }
    }

    this.logger.log(`CSV replay complete — ${count} events published`);
  }

  // ── TypeScript event simulator ─────────────────────────────────────────────
  // Generates realistic synthetic events with:
  // - Morning (6–9am) and evening (5–8pm) peak hour spikes
  // - Weekend vs weekday variation
  // - Workout type weighted by gym facilities
  async runSimulator(eventsPerMinute = 60, durationMins = 10): Promise<void> {
    const gymIds = [
      'gym_1',
      'gym_2',
      'gym_3',
      'gym_4',
      'gym_5',
      'gym_6',
      'gym_7',
      'gym_8',
      'gym_9',
      'gym_10',
    ];
    const workoutTypes: WorkoutType[] = [
      'Weightlifting',
      'Swimming',
      'Pilates',
      'CrossFit',
      'Yoga',
      'Cardio',
    ];

    const peakHours = [6, 7, 8, 9, 17, 18, 19, 20]; // higher probability in morning + evening
    const totalEvents = eventsPerMinute * durationMins;
    const delayMs = (60 * 1000) / eventsPerMinute;

    this.logger.log(
      `Simulator starting — ${totalEvents} events over ${durationMins} mins`,
    );

    for (let i = 0; i < totalEvents; i++) {
      const userId = `user_${Math.floor(Math.random() * 5000) + 1}`;
      const gymId = gymIds[Math.floor(Math.random() * gymIds.length)];
      const workout =
        workoutTypes[Math.floor(Math.random() * workoutTypes.length)];

      // Bias toward peak hours
      const hour =
        Math.random() < 0.6
          ? peakHours[Math.floor(Math.random() * peakHours.length)]
          : Math.floor(Math.random() * 15) + 6; // 6am–9pm range

      const now = new Date();
      const checkinTime = new Date(now);
      checkinTime.setHours(hour, Math.floor(Math.random() * 60), 0, 0);

      const durationMinsVal = 30 + Math.floor(Math.random() * 90); // 30–120 mins
      const checkoutTime = new Date(
        checkinTime.getTime() + durationMinsVal * 60000,
      );

      const event: CheckinEvent = {
        eventId: uuidv4(),
        userId,
        gymId,
        checkinTime: checkinTime.toISOString(),
        checkoutTime: checkoutTime.toISOString(),
        workoutType: workout,
        caloriesBurned: 300 + Math.random() * 800,
        durationMins: durationMinsVal,
        source: 'simulator',
      };

      await this.publishCheckinEvent(event);
      await this.sleep(delayMs);
    }

    this.logger.log('Simulator complete');
  }

  private sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
}